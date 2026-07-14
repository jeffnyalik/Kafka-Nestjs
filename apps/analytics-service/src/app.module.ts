import { KafkaModule } from "@app/kafka";
import { Module } from "@nestjs/common";
import { AnalyticsServiceService } from "./analytics-service.service";

@Module({
    imports: [
        KafkaModule.register({
            clientId: 'analytics-service',
            groupId: 'analytics-service',
            brokers: ['localhost:9092'],
        })
    ],
    providers: [
        AnalyticsServiceService
    ]
})
export class AppModule {}