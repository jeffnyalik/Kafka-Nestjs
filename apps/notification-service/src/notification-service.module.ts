import { Module } from '@nestjs/common';
import { KafkaModule } from '@app/kafka';
import { NotificationServiceController } from './notification-service.controller';
import { NotificationServiceService } from './notification-service.service';

@Module({
  imports: [
    KafkaModule.register({
      clientId: 'notification-service',
      brokers: ['localhost:9092'],
      groupId: 'notification-group',
    }),
  ],
  controllers: [NotificationServiceController],
  providers: [NotificationServiceService],
})
export class NotificationServiceModule {}
