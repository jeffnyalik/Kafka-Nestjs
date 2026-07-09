import { Module } from '@nestjs/common';
import { KafkaModule } from '@app/kafka';
import { TicketServiceController } from './ticket-service.controller';
import { TicketServiceService } from './ticket-service.service';

@Module({
  imports: [KafkaModule],
  controllers: [TicketServiceController],
  providers: [TicketServiceService],
})
export class TicketServiceModule {}
