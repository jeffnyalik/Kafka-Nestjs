import { Injectable, Logger, OnApplicationBootstrap, OnModuleDestroy, OnModuleInit } from "@nestjs/common";
import { Consumer, Kafka } from "kafkajs";
import { kafkaConfig } from "../config/kafka.config";

const RECONNECT_DELAY_MS = 5000;

@Injectable()
export class KafkaConsumerService implements OnApplicationBootstrap, OnModuleDestroy{
    private readonly logger = new Logger(KafkaConsumerService.name)
    private readonly kafka = new Kafka(kafkaConfig);
    private isConnected = false;
    private destroyed = false;
    private reconnectTimer?: NodeJS.Timeout;

    // This is the consumer instance
    private readonly consumer: Consumer = this.kafka.consumer({
        groupId: 'notification-group'
    })

    // This method is called when the module is initialized
    onApplicationBootstrap(){
        void this.connect();
    }

    // This method is called when the module is initialized
    private async connect(): Promise<void> {
        if (this.destroyed || this.isConnected) {
            return;
        }

        try {
            await this.consumer.connect();
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
    // This method is called when the connection fails
    private scheduleReconnect(): void {
        if (this.destroyed || this.reconnectTimer) {
            return;
        }

        this.reconnectTimer = setTimeout(() => {
            this.reconnectTimer = undefined;
            void this.connect();
        }, RECONNECT_DELAY_MS);
    }
    
    // This method is called when the module is destroyed
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
            await this.consumer.disconnect();
            this.logger.log('Kafka producer is disconnected');
        } catch (error) {
            this.logger.warn(
                `Failed to disconnect Kafka producer: ${error instanceof Error ? error.message : error}`,
            );
        } finally {
            this.isConnected = false;
        }
    }

    // this method is used to subscribe to a topic
    async subscribe(
        topic: string,
        handler: (message: unknown) => Promise<void>,
    ){
        await this.consumer.subscribe({ topic, fromBeginning: true });
        await this.consumer.run({
            eachMessage: async ({ message }) => {
                if(!message.value){return;}
                const payload = JSON.parse(message.value.toString());
                await handler(payload);
            }
         })
    }

}