import { Inject, Injectable, Logger, OnApplicationBootstrap, OnModuleDestroy } from '@nestjs/common';
import { Kafka, Producer } from 'kafkajs';
import { KAFKA_MODULE_OPTIONS } from '../constants/kafka.constants';
import type { KafkaModuleOptions } from '../interfaces/kafka-module-options.interface';

const RECONNECT_DELAY_MS = 5000;
const DEFAULT_RETRY = { retries: 2, initialRetryTime: 100 };

@Injectable()
export class KafkaProducerService implements OnApplicationBootstrap, OnModuleDestroy {
    private readonly logger = new Logger(KafkaProducerService.name);
    private readonly kafka: Kafka;
    private readonly producer: Producer;

    private isConnected = false;
    private destroyed = false;
    private reconnectTimer?: NodeJS.Timeout;

    constructor(
        @Inject(KAFKA_MODULE_OPTIONS)
        private readonly options: KafkaModuleOptions,
    ) {
        this.kafka = new Kafka({
            clientId: this.options.clientId,
            brokers: this.options.brokers,
            retry: this.options.retry ?? DEFAULT_RETRY,
        });
        this.producer = this.kafka.producer();
    }

    onApplicationBootstrap() {
        void this.connect();
    }

    private async connect(): Promise<void> {
        if (this.destroyed || this.isConnected) {
            return;
        }

        try {
            await this.producer.connect();
            this.isConnected = true;
            this.logger.log('Kafka producer is connected');
        } catch (error) {
            this.isConnected = false;
            this.logger.warn(
                `Kafka producer is unavailable (${error instanceof Error ? error.message : error}), retrying in ${RECONNECT_DELAY_MS / 1000}s`,
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
            await this.producer.disconnect();
            this.logger.log('Kafka producer is disconnected');
        } catch (error) {
            this.logger.warn(
                `Failed to disconnect Kafka producer: ${error instanceof Error ? error.message : error}`,
            );
        } finally {
            this.isConnected = false;
        }
    }

    async publish(topic: string, message: unknown): Promise<void> {
        if (!this.isConnected) {
            this.logger.warn(`Cannot publish to topic ${topic}: Kafka producer is not connected`);
            throw new Error('Kafka producer is not connected');
        }

        try {
            await this.producer.send({
                topic,
                messages: [{ value: JSON.stringify(message) }],
            });
            this.logger.debug(`Message published to topic: ${topic}`);
        } catch (error) {
            this.isConnected = false;
            this.logger.error(`Error publishing message to topic: ${topic}`);
            this.scheduleReconnect();
            throw error;
        }
    }
}
