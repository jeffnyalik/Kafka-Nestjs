import { Module } from '@nestjs/common';
import { TicketServiceController } from './ticket-service.controller';
import { TicketServiceService } from './ticket-service.service';

@Module({
  imports: [],
  controllers: [TicketServiceController],
  providers: [TicketServiceService],
})
export class TicketServiceModule {}
