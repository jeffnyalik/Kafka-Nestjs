import { TicketCreatedEvent } from '@app/events/ticket/ticket-created.event';
import { KAFKA_TOPICS, KafkaProducerService } from '@app/kafka';
import { Injectable } from '@nestjs/common';
import { randomUUID } from 'crypto';
import { CreateTicketDto } from './dto/create-ticket.dto';

@Injectable()
export class TicketServiceService {
  constructor(private readonly kafkaProducer: KafkaProducerService){}
  
  async createTicket(dto: CreateTicketDto){
    const event: TicketCreatedEvent = {
      eventId: randomUUID(),
      ticketId: randomUUID(),
      title: dto.title,
      description: dto.description,
      priority: dto.priority,
      occuredAt: new Date()
    };

    await this.kafkaProducer.publish(KAFKA_TOPICS.TICKET_EVENTS, event);
    return {
      message: "Ticket created successfully",
      eventId: event.eventId, 
    }
  }
}  
