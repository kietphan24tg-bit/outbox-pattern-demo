import { Inject, Injectable, Logger } from '@nestjs/common';
import { Connection } from 'amqplib';
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

@Injectable()
export class Publisher {
    private static readonly RETRY_DELAY_SECONDS = 30;

    private readonly logger = new Logger(Publisher.name);

    constructor(
        @Inject('DatabasePool') private readonly databaseClient: Pool,
        @Inject('RabbitMqClient') private readonly rabbitMqClient: Connection
    ) {}

    async publish(record: OutboxRecord): Promise<void> {
        const channel = await this.rabbitMqClient.createConfirmChannel();

        try {
            const payloadBuffer = Buffer.from(this.serializePayload(record));

            channel.publish(
                record.exchange_name,
                record.routing_key,
                payloadBuffer
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

            await this.databaseClient.query(
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
        `,
                [
                    Status.FAILED,
                    record.id,
                    errorMessage,
                    Publisher.RETRY_DELAY_SECONDS
                ]
            );
        } finally {
            await channel.close();
        }
    }

    private serializePayload(record: OutboxRecord): string {
        if (typeof record.payload === 'string') {
            return record.payload;
        }

        return JSON.stringify(record.payload);
    }
}
