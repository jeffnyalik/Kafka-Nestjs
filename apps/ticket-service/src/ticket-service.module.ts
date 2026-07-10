import { Module } from '@nestjs/common';
import { KafkaModule } from '@app/kafka';
import { TicketServiceController } from './tickets/ticket-service.controller';
import { TicketServiceService } from './tickets/ticket-service.service';

@Module({
  imports: [KafkaModule],
  controllers: [TicketServiceController],
  providers: [TicketServiceService],
})
export class TicketServiceModule {}
