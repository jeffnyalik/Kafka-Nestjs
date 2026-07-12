import {
    TICKET_EVENT_TYPES,
    TicketCreatedPayload,
    TicketEventEnvelope,
} from '@app/events';
import { KAFKA_TOPICS, KafkaProducerService } from '@app/kafka';
import { Injectable } from '@nestjs/common';
import { randomUUID } from 'crypto';
import { CreateTicketDto } from './dto/create-ticket.dto';

@Injectable()
export class TicketServiceService {
    constructor(private readonly kafkaProducer: KafkaProducerService) {}

    async createTicket(dto: CreateTicketDto) {
        const ticketId = randomUUID();
        const event: TicketEventEnvelope<TicketCreatedPayload> = {
            type: TICKET_EVENT_TYPES.CREATED,
            eventId: randomUUID(),
            ticketId,
            occurredAt: new Date(),
            payload: {
                title: dto.title,
                description: dto.description,
                priority: dto.priority,
            },
        };

        await this.kafkaProducer.publish(KAFKA_TOPICS.TICKET_EVENTS, event);
        return {
            message: 'Ticket created successfully',
            eventId: event.eventId,
            ticketId,
        };
    }
}
