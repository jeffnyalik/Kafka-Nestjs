import { Inject, Injectable, Logger, OnApplicationBootstrap, OnModuleDestroy } from '@nestjs/common';
import { Consumer, Kafka } from 'kafkajs';
import { KAFKA_MODULE_OPTIONS } from '../constants/kafka.constants';
import type { KafkaModuleOptions } from '../interfaces/kafka-module-options.interface';

const RECONNECT_DELAY_MS = 5000;
const DEFAULT_RETRY = { retries: 2, initialRetryTime: 100 };

@Injectable()
export class KafkaConsumerService implements OnApplicationBootstrap, OnModuleDestroy {
    private readonly logger = new Logger(KafkaConsumerService.name);
    private readonly kafka: Kafka;
    private readonly consumer: Consumer;
    private readonly handlers = new Map<string, (message: unknown) => Promise<void>>();

    private isConnected = false;
    private isRunning = false;
    private destroyed = false;
    private reconnectTimer?: NodeJS.Timeout;

    constructor(
        @Inject(KAFKA_MODULE_OPTIONS)
        private readonly options: KafkaModuleOptions,
    ) {
        if (!this.options.groupId) {
            throw new Error('groupId is required for KafkaConsumerService');
        }

        this.kafka = new Kafka({
            clientId: this.options.clientId,
            brokers: this.options.brokers,
            retry: this.options.retry ?? DEFAULT_RETRY,
        });
        this.consumer = this.kafka.consumer({ groupId: this.options.groupId });
    }

    onApplicationBootstrap() {
        void this.connect();
    }

    private async connect(): Promise<void> {
        if (this.destroyed || this.isConnected) {
            return;
        }

        try {
            await this.consumer.connect();
            this.isConnected = true;
            this.logger.log('Kafka consumer is connected');
        } catch (error) {
            this.isConnected = false;
            this.logger.warn(
                `Kafka consumer is unavailable (${error instanceof Error ? error.message : error}), retrying in ${RECONNECT_DELAY_MS / 1000}s`,
            );
            this.scheduleReconnect();
        }
    }

    private scheduleReconnect(): void {
        if (this.destroyed || this.reconnectTimer) {
            return;
        }

        this.reconnectTimer = setTimeout(() => {
            this.reconnectTimer = undefined;
            void this.connect();
        }, RECONNECT_DELAY_MS);
    }

    async onModuleDestroy() {
        this.destroyed = true;

        if (this.reconnectTimer) {
            clearTimeout(this.reconnectTimer);
            this.reconnectTimer = undefined;
        }

        if (!this.isConnected) {
            return;
        }

        try {
            if (this.isRunning) {
                await this.consumer.stop();
                this.isRunning = false;
            }

            await this.consumer.disconnect();
            this.logger.log('Kafka consumer is disconnected');
        } catch (error) {
            this.logger.warn(
                `Failed to disconnect Kafka consumer: ${error instanceof Error ? error.message : error}`,
            );
        } finally {
            this.isConnected = false;
            this.handlers.clear();
        }
    }

    async subscribe(
        topic: string,
        handler: (message: unknown) => Promise<void>,
    ): Promise<void> {
        if (!this.isConnected) {
            this.logger.warn(`Cannot subscribe to topic ${topic}: Kafka consumer is not connected`);
            throw new Error('Kafka consumer is not connected');
        }

        this.handlers.set(topic, handler);
        await this.consumer.subscribe({ topic, fromBeginning: true });

        if (!this.isRunning) {
            this.isRunning = true;
            await this.consumer.run({
                eachMessage: async ({ topic: messageTopic, message }) => {
                    if (!message.value) {
                        return;
                    }

                    const topicHandler = this.handlers.get(messageTopic);
                    if (!topicHandler) {
                        return;
                    }

                    const payload = JSON.parse(message.value.toString());
                    await topicHandler(payload);
                },
            });
        }

        this.logger.log(`Subscribed to topic ${topic}`);
    }

    async unsubscribe(topic: string): Promise<void> {
        this.handlers.delete(topic);
        this.logger.log(`Removed handler for topic ${topic}`);
    }
}
