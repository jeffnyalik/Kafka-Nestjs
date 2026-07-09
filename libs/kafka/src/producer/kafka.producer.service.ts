import { Injectable, Logger, OnApplicationBootstrap, OnModuleDestroy } from "@nestjs/common";
import { Kafka, Producer } from "kafkajs";
import { kafkaConfig } from "../config/kafka.config";

@Injectable()
export class KafkaProducerService implements OnApplicationBootstrap, OnModuleDestroy {
    private readonly logger = new Logger(KafkaProducerService.name);

    private readonly kafka = new Kafka(kafkaConfig);

    private readonly producer: Producer = this.kafka.producer();

    async onApplicationBootstrap() {
        await this.producer.connect();
        this.logger.log('Kafka producer is connected');
    }
    async onModuleDestroy(){
        await this.producer.disconnect();
        this.logger.log('Kafka producer is disconnected');
    }

    async publish(topic: string, message: unknown): Promise<any>{
        try {
            await this.producer.send({
                topic,
                messages: [
                    {value: JSON.stringify(message)}
                ]
            });
            this.logger.debug(`Message sent to published topic: ${topic}`);
        } catch(error){
            this.logger.error(`Error publishing message to topic: ${topic}`);
            throw error;
        }
    }
}