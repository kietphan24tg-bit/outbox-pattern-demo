import {
    Inject,
    Injectable,
    Logger,
    OnModuleDestroy,
    OnModuleInit
} from '@nestjs/common';
import { Client, Pool } from 'pg';
import { OutboxRecord, Publisher, Status } from './service/publisher';

type ReadyOutboxRecord = OutboxRecord & {
    status: Status;
};

type PgMessage = {
    payload?: string;
};

type PgPayload = {
    data: ReadyOutboxRecord;
};

@Injectable()
export class OutboxListener implements OnModuleInit, OnModuleDestroy {
    private static readonly STALE_PROCESSING_TIMEOUT_MS = 60_000;
    private static readonly RECOVERY_INTERVAL_MS = 30_000;
    private static readonly MAX_RETRY_COUNT = 5;

    private readonly logger = new Logger(OutboxListener.name);
    private recoveryTimer?: NodeJS.Timeout;

    constructor(
        @Inject('DatabasePool') private readonly databasePool: Pool,
        @Inject('DatabaseListenerClient')
        private readonly listenerClient: Client,
        private readonly publisher: Publisher
    ) {}

    async onModuleInit(): Promise<void> {
        await this.runRecoveryCycle();
        await this.processAllReadyMessages();
        this.startRecoveryLoop();

        await this.listenerClient.query('LISTEN outbox_channel');
        this.logger.log('Listening on outbox_channel');

        this.listenerClient.on('notification', async (msg: PgMessage) => {
            try {
                const record = this.parseReadyRecord(msg);

                if (!record) {
                    return;
                }

                if (record.status !== Status.READY) {
                    return;
                }

                this.logger.debug(`Handled record [${JSON.stringify(record)}]`);
                await this.claimAndPublishRecord(record.id);
            } catch (error) {
                const errorMessage =
                    error instanceof Error ? error.message : String(error);

                this.logger.error(
                    `Failed to handle notification payload: ${errorMessage}`
                );
            }
        });
    }

    onModuleDestroy(): void {
        if (!this.recoveryTimer) {
            return;
        }

        clearInterval(this.recoveryTimer);
        this.recoveryTimer = undefined;
    }

    private async processAllReadyMessages(): Promise<void> {
        const result = await this.databasePool.query<ReadyOutboxRecord>(
            'SELECT * FROM outbox_messages WHERE status = $1 ORDER BY created_at ASC',
            [Status.READY]
        );

        for (const record of result.rows) {
            await this.claimAndPublishRecord(record.id);
        }

        this.logger.log(`Processed READY messages [${result.rows.length}]`);
    }

    private parseReadyRecord(msg: PgMessage): ReadyOutboxRecord | null {
        if (!msg.payload) {
            return null;
        }

        let parsedPayload: unknown;

        try {
            parsedPayload = JSON.parse(msg.payload);
        } catch {
            this.logger.warn(
                `Skipped invalid JSON notification payload [${msg.payload}]`
            );
            return null;
        }

        if (!this.isPgPayload(parsedPayload)) {
            this.logger.warn(
                `Skipped unexpected notification payload shape [${msg.payload}]`
            );
            return null;
        }

        return parsedPayload.data;
    }

    private isPgPayload(payload: unknown): payload is PgPayload {
        if (!payload || typeof payload !== 'object') {
            return false;
        }

        const candidate = payload as { data?: unknown };

        return this.isReadyOutboxRecord(candidate.data);
    }

    private isReadyOutboxRecord(data: unknown): data is ReadyOutboxRecord {
        if (!data || typeof data !== 'object') {
            return false;
        }

        const candidate = data as Partial<ReadyOutboxRecord>;

        return (
            typeof candidate.id === 'string' &&
            typeof candidate.status === 'string' &&
            Object.values(Status).includes(candidate.status as Status) &&
            typeof candidate.exchange_name === 'string' &&
            typeof candidate.routing_key === 'string' &&
            candidate.payload !== undefined
        );
    }

    private async claimAndPublishRecord(recordId: string): Promise<void> {
        const claimedRecord = await this.claimReadyRecord(recordId);

        if (!claimedRecord) {
            this.logger.debug(
                `Skipped record [${recordId}] because it was already claimed`
            );
            return;
        }

        await this.publisher.publish(claimedRecord);
    }

    private async claimReadyRecord(
        recordId: string
    ): Promise<OutboxRecord | null> {
        const result = await this.databasePool.query<OutboxRecord>(
            `
                UPDATE outbox_messages
                SET status = $1, processing_at = NOW()
                WHERE id = $2 AND status = $3
                RETURNING id, exchange_name, routing_key, payload
            `,
            [Status.PROCESSING, recordId, Status.READY]
        );

        return result.rows[0] ?? null;
    }

    private startRecoveryLoop(): void {
        this.recoveryTimer = setInterval(() => {
            void this.runRecoveryCycle();
        }, OutboxListener.RECOVERY_INTERVAL_MS);
    }

    private async runRecoveryCycle(): Promise<void> {
        await this.requeueStaleProcessingRecords();
        await this.requeueRetryableFailedRecords();
    }

    private async requeueStaleProcessingRecords(): Promise<void> {
        const result = await this.databasePool.query<{ id: string }>(
            `
                UPDATE outbox_messages
                SET status = $1, processing_at = NULL
                WHERE status = $2
                  AND processing_at IS NOT NULL
                  AND processing_at < NOW() - ($3 * INTERVAL '1 millisecond')
                RETURNING id
            `,
            [
                Status.READY,
                Status.PROCESSING,
                OutboxListener.STALE_PROCESSING_TIMEOUT_MS
            ]
        );

        if (!result.rowCount) {
            return;
        }

        this.logger.warn(
            `Requeued stale PROCESSING messages [${result.rowCount}]`
        );

        for (const record of result.rows) {
            await this.claimAndPublishRecord(record.id);
        }
    }

    private async requeueRetryableFailedRecords(): Promise<void> {
        const result = await this.databasePool.query<{ id: string }>(
            `
                UPDATE outbox_messages
                SET status = $1,
                    failed_at = NULL,
                    next_retry_at = NULL
                WHERE status = $2
                  AND next_retry_at IS NOT NULL
                  AND next_retry_at <= NOW()
                  AND COALESCE(retry_count, 0) < $3
                RETURNING id
            `,
            [Status.READY, Status.FAILED, OutboxListener.MAX_RETRY_COUNT]
        );

        if (!result.rowCount) {
            return;
        }

        this.logger.warn(
            `Requeued FAILED messages for retry [${result.rowCount}]`
        );

        for (const record of result.rows) {
            await this.claimAndPublishRecord(record.id);
        }
    }
}
