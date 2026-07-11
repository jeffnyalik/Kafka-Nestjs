import { KafkaModule } from "@app/kafka";
import { Module } from "@nestjs/common";

@Module({
    imports: [
        KafkaModule.register({
            clientId: 'notification-service',
            groupId: 'notification-service',
            brokers: ['localhost:9092'],
        })
    ]
})
export class AppModule {}