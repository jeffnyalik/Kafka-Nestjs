import { Module } from '@nestjs/common';
import { KafkaModule } from '@app/kafka';
import { TicketServiceController } from './tickets/ticket-service.controller';
import { TicketServiceService } from './tickets/ticket-service.service';

@Module({
  imports: [
    KafkaModule.register({
      clientId: 'ticket-service',
      brokers: ['localhost:9092'],
    }),
  ],
  controllers: [TicketServiceController],
  providers: [TicketServiceService],
})
export class TicketServiceModule {}
