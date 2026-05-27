import { Inject, Injectable, Logger } from '@nestjs/common';
import { Connection } from 'amqplib';
import { Pool } from 'pg';

export enum Status {
    READY = 'READY',
    FAILED = 'FAILED'
}

export type OutboxRecord = {
    id: string;
    exchange_name: string;
    routing_key: string;
    payload: string;
};

@Injectable()
export class Publisher {
    private readonly logger = new Logger(Publisher.name);

    constructor(
        @Inject('DatabasePool') private readonly databaseClient: Pool,
        @Inject('RabbitMqClient') private readonly rabbitMqClient: Connection
    ) {}

    async publish(record: OutboxRecord): Promise<void> {
        const channel = await this.rabbitMqClient.createConfirmChannel();

        try {
            channel.publish(
                record.exchange_name,
                record.routing_key,
                Buffer.from(record.payload)
            );

            await channel.waitForConfirms();

            await this.databaseClient.query(
                'DELETE FROM outbox_messages WHERE id = $1',
                [record.id]
            );

            this.logger.debug(`Consumed record with ID [${record.id}]`);
        } catch (err) {
            this.logger.error(
                `Failed to send message to RabbitMQ for record ID [${record.id}]: ${err}`
            );

            await this.databaseClient.query(
                `
          UPDATE outbox_messages
          SET status = $1, failed_at = NOW()
          WHERE id = $2
        `,
                [Status.FAILED, record.id]
            );
        } finally {
            await channel.close();
        }
    }
}
