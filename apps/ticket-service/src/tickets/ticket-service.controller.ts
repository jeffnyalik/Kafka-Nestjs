import { Body, Controller, Get, Post } from '@nestjs/common';
import { CreateTicketDto } from './dto/create-ticket.dto';
import { TicketServiceService } from './ticket-service.service';

@Controller('tickets')
export class TicketServiceController {
  constructor(private readonly ticketServiceService: TicketServiceService) {}
  @Post()
  async createTicket(@Body() dto: CreateTicketDto){
    return this.ticketServiceService.createTicket(dto);
  }
}
