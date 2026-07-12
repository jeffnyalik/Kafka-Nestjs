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
    private readonly subscribedTopics = new Set<string>();

    private isConnected = false;
    private isRunning = false;
    private destroyed = false;
    private reconnectTimer?: NodeJS.Timeout;
    private connecting: Promise<boolean> | null = null;

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
        void this.ensureConnected();
    }

    private ensureConnected(): Promise<boolean> {
        if (this.isConnected) {
            return Promise.resolve(true);
        }

        if (!this.connecting) {
            this.connecting = this.connect();
        }

        return this.connecting;
    }

    private async connect(): Promise<boolean> {
        if (this.destroyed || this.isConnected) {
            return this.isConnected;
        }

        try {
            await this.consumer.connect();
            this.isConnected = true;
            this.logger.log('Kafka consumer is connected');
            await this.activateSubscriptions();
            return true;
        } catch (error) {
            this.isConnected = false;
            this.subscribedTopics.clear();
            this.isRunning = false;
            this.logger.warn(
                `Kafka consumer is unavailable (${error instanceof Error ? error.message : error}), retrying in ${RECONNECT_DELAY_MS / 1000}s`,
            );
            this.scheduleReconnect();
            return false;
        } finally {
            this.connecting = null;
        }
    }

    private scheduleReconnect(): void {
        if (this.destroyed || this.reconnectTimer) {
            return;
        }

        this.reconnectTimer = setTimeout(() => {
            this.reconnectTimer = undefined;
            void this.ensureConnected();
        }, RECONNECT_DELAY_MS);
    }

    private async activateSubscriptions(): Promise<void> {
        for (const topic of this.handlers.keys()) {
            await this.addTopicSubscription(topic);
        }
    }

    private async addTopicSubscription(topic: string): Promise<void> {
        if (this.subscribedTopics.has(topic)) {
            return;
        }

        await this.consumer.subscribe({ topic, fromBeginning: false });
        this.subscribedTopics.add(topic);

        if (!this.isRunning) {
            this.isRunning = true;
            await this.consumer.run({
                eachMessage: async ({ topic: messageTopic, partition, message }) => {
                    if (!message.value) {
                        return;
                    }

                    const topicHandler = this.handlers.get(messageTopic);
                    if (!topicHandler) {
                        return;
                    }

                    this.logger.log(
                        `Received message [topic=${messageTopic} partition=${partition} offset=${message.offset}]`,
                    );

                    const payload = JSON.parse(message.value.toString());
                    await topicHandler(payload);
                },
            });
        }
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
            this.subscribedTopics.clear();
            this.handlers.clear();
        }
    }

    async subscribe(
        topic: string,
        handler: (message: unknown) => Promise<void>,
    ): Promise<void> {
        this.handlers.set(topic, handler);

        const connected = await this.ensureConnected();
        if (!connected) {
            this.logger.warn(`Subscription to ${topic} queued until Kafka consumer connects`);
            return;
        }

        await this.addTopicSubscription(topic);
        this.logger.log(`Subscribed to topic ${topic}`);
    }

    async unsubscribe(topic: string): Promise<void> {
        this.handlers.delete(topic);
        this.logger.log(`Removed handler for topic ${topic}`);
    }
}
