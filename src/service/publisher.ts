import { Inject, Injectable, Logger, OnModuleDestroy } from '@nestjs/common';
import { ConfirmChannel, Connection } from 'amqplib';
import { Pool } from 'pg';

export enum Status {
    READY = 'READY',
    PROCESSING = 'PROCESSING',
    FAILED = 'FAILED'
}

export type OutboxRecord = {
    id: string;
    exchange_name: string;
    routing_key: string;
    payload: unknown;
};

type FailedOutboxRecord = {
    next_retry_at: Date;
    retry_count: number;
};

@Injectable()
export class Publisher implements OnModuleDestroy {
    private static readonly RETRY_DELAY_SECONDS = 30;

    private readonly logger = new Logger(Publisher.name);
    private channelPromise?: Promise<ConfirmChannel>;

    constructor(
        @Inject('DatabasePool') private readonly databaseClient: Pool,
        @Inject('RabbitMqClient') private readonly rabbitMqClient: Connection
    ) {}

    async onModuleDestroy(): Promise<void> {
        if (!this.channelPromise) {
            return;
        }

        const channel = await this.channelPromise;
        this.channelPromise = undefined;
        await channel.close();
    }

    async publish(record: OutboxRecord): Promise<void> {
        const channel = await this.getChannel();

        try {
            const payloadBuffer = Buffer.from(this.serializePayload(record));

            channel.publish(
                record.exchange_name,
                record.routing_key,
                payloadBuffer,
                {
                    contentType: 'application/json',
                    deliveryMode: 2,
                    messageId: record.id,
                    timestamp: Date.now(),
                    type: record.routing_key,
                    headers: {
                        eventId: record.id
                    }
                }
            );

            await channel.waitForConfirms();

            await this.databaseClient.query(
                'DELETE FROM outbox_messages WHERE id = $1',
                [record.id]
            );

            this.logger.debug(`Consumed record with ID [${record.id}]`);
        } catch (err) {
            const errorMessage =
                err instanceof Error ? err.message : String(err);

            this.logger.error(
                `Failed to send message to RabbitMQ for record ID [${record.id}]: ${errorMessage}`
            );

            const result = await this.databaseClient.query<FailedOutboxRecord>(
                `
          UPDATE outbox_messages
          SET status = $1,
              failed_at = NOW(),
              processing_at = NULL,
              retry_count = COALESCE(retry_count, 0) + 1,
              last_error = $3,
              next_retry_at = NOW() + (
                  POWER(2, LEAST(COALESCE(retry_count, 0), 10)) * $4 * INTERVAL '1 second'
              )
          WHERE id = $2
          RETURNING retry_count, next_retry_at
        `,
                [
                    Status.FAILED,
                    record.id,
                    errorMessage,
                  Publisher.RETRY_DELAY_SECONDS
                ]
            );

            const failedRecord = result.rows[0];

            if (failedRecord) {
                this.logger.warn(
                    `Scheduled retry for record [${record.id}] [retryCount=${failedRecord.retry_count}] [nextRetryAt=${failedRecord.next_retry_at.toISOString()}]`
                );
            }
        }
    }

    private async getChannel(): Promise<ConfirmChannel> {
        if (!this.channelPromise) {
            this.channelPromise = this.rabbitMqClient
                .createConfirmChannel()
                .then(channel => {
                    channel.on('close', () => {
                        this.channelPromise = undefined;
                    });

                    channel.on('error', error => {
                        this.logger.warn(
                            `RabbitMQ confirm channel error: ${error.message}`
                        );
                        this.channelPromise = undefined;
                    });

                    return channel;
                });
        }

        return this.channelPromise;
    }

    private serializePayload(record: OutboxRecord): string {
        if (typeof record.payload === 'string') {
            return record.payload;
        }

        return JSON.stringify(record.payload);
    }
}
