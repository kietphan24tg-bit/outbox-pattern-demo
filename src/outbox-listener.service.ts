import {
    Inject,
    Injectable,
    Logger,
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
export class OutboxListener implements OnModuleInit {
    private readonly logger = new Logger(OutboxListener.name);

    constructor(
        @Inject('DatabasePool') private readonly databasePool: Pool,
        @Inject('DatabaseListenerClient')
        private readonly listenerClient: Client,
        private readonly publisher: Publisher
    ) {}

    async onModuleInit(): Promise<void> {
        await this.processAllReadyMessages();

        await this.listenerClient.query('LISTEN outbox_channel');
        this.logger.log('Listening on outbox_channel');

        this.listenerClient.on('notification', async (msg: PgMessage) => {
            if (!msg.payload) {
                return;
            }

            const payload = JSON.parse(msg.payload) as PgPayload;
            const record = payload.data;

            if (record.status !== Status.READY) {
                return;
            }

            this.logger.debug(`Handled record [${JSON.stringify(record)}]`);
            await this.publisher.publish(record);
        });
    }

    private async processAllReadyMessages(): Promise<void> {
        const result = await this.databasePool.query<ReadyOutboxRecord>(
            'SELECT * FROM outbox_messages WHERE status = $1 ORDER BY created_at ASC',
            [Status.READY]
        );

        for (const record of result.rows) {
            await this.publisher.publish(record);
        }

        this.logger.log(`Processed READY messages [${result.rows.length}]`);
    }
}
