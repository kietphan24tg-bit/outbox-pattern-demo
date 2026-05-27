import { Module } from '@nestjs/common';
import { ConfigModule, ConfigService } from '@nestjs/config';
import { connect } from 'amqplib';
import { Client, Pool } from 'pg';
import { AppController } from './app.controller';
import { AppService } from './app.service';
import { OutboxListener } from './outbox-listener.service';
import { Publisher } from './publisher';

@Module({
    imports: [ConfigModule.forRoot({ isGlobal: true })],
    controllers: [AppController],
    providers: [
        AppService,
        Publisher,
        OutboxListener,
        {
            provide: 'DatabasePool',
            useFactory: (config: ConfigService) => {
                const connectionString =
                    config.getOrThrow<string>('DATABASE_URL');
                return new Pool({ connectionString });
            },
            inject: [ConfigService]
        },
        {
            provide: 'DatabaseListenerClient',
            useFactory: async (config: ConfigService) => {
                const connectionString =
                    config.getOrThrow<string>('DATABASE_URL');
                const client = new Client({ connectionString });
                await client.connect();
                return client;
            },
            inject: [ConfigService]
        },
        {
            provide: 'RabbitMqClient',
            useFactory: async (config: ConfigService) => {
                const rabbitMqUrl = config.getOrThrow<string>('RABBITMQ_URL');
                return connect(rabbitMqUrl);
            },
            inject: [ConfigService]
        }
    ],
    exports: [
        Publisher,
        'DatabasePool',
        'DatabaseListenerClient',
        'RabbitMqClient'
    ]
})
export class AppModule {}
