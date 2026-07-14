import { Module } from '@nestjs/common';
import { AnalyticsServiceController } from './analytics-service.controller';
import { AnalyticsServiceService } from './analytics-service.service';
import { KafkaModule } from '@app/kafka';

@Module({
  imports: [
    KafkaModule.register({
      clientId: 'analytics-service',
      brokers: ['localhost: 9092'],
      groupId: 'analytics-service'
    }),
  ],
  controllers: [AnalyticsServiceController],
  providers: [AnalyticsServiceService],
})
export class AnalyticsServiceModule {}
