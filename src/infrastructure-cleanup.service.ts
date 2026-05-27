import {
    Inject,
    Injectable,
    Logger,
    OnApplicationShutdown
} from '@nestjs/common';
import { Connection } from 'amqplib';
import { Client, Pool } from 'pg';

@Injectable()
export class InfrastructureCleanupService implements OnApplicationShutdown {
    private readonly logger = new Logger(InfrastructureCleanupService.name);
    private isShuttingDown = false;

    constructor(
        @Inject('DatabasePool') private readonly databasePool: Pool,
        @Inject('DatabaseListenerClient')
        private readonly listenerClient: Client,
        @Inject('RabbitMqClient')
        private readonly rabbitMqClient: Connection
    ) {}

    async onApplicationShutdown(signal?: string): Promise<void> {
        if (this.isShuttingDown) {
            return;
        }

        this.isShuttingDown = true;
        this.logger.log(
            `Shutting down infrastructure resources${signal ? ` [${signal}]` : ''}`
        );

        await this.unlistenOutboxChannel();
        await this.closeListenerClient();
        await this.closeDatabasePool();
        await this.closeRabbitMqConnection();
    }

    private async unlistenOutboxChannel(): Promise<void> {
        try {
            await this.listenerClient.query('UNLISTEN outbox_channel');
        } catch (error) {
            this.logger.warn(
                `Failed to UNLISTEN outbox_channel: ${this.getErrorMessage(error)}`
            );
        }
    }

    private async closeListenerClient(): Promise<void> {
        try {
            await this.listenerClient.end();
        } catch (error) {
            this.logger.warn(
                `Failed to close DatabaseListenerClient: ${this.getErrorMessage(error)}`
            );
        }
    }

    private async closeDatabasePool(): Promise<void> {
        try {
            await this.databasePool.end();
        } catch (error) {
            this.logger.warn(
                `Failed to close DatabasePool: ${this.getErrorMessage(error)}`
            );
        }
    }

    private async closeRabbitMqConnection(): Promise<void> {
        try {
            await this.rabbitMqClient.close();
        } catch (error) {
            this.logger.warn(
                `Failed to close RabbitMqClient: ${this.getErrorMessage(error)}`
            );
        }
    }

    private getErrorMessage(error: unknown): string {
        return error instanceof Error ? error.message : String(error);
    }
}
