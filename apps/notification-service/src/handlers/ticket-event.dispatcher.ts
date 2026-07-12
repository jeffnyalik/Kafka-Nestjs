import {
    TICKET_EVENT_TYPES,
    TicketAssignedPayload,
    TicketClosedPayload,
    TicketCreatedPayload,
    TicketEventEnvelope,
    TicketEventType,
    TicketResolvedPayload,
} from '@app/events';
import { Injectable, Logger } from '@nestjs/common';
import { TicketAssignedHandler } from './ticket-assigned.handler';
import { TicketClosedHandler } from './ticket-closed.handler';
import { TicketCreatedHandler } from './ticket-created.handler';
import { TicketResolvedHandler } from './ticket-resolved.handler';

@Injectable()
export class TicketEventDispatcher {
    private readonly logger = new Logger(TicketEventDispatcher.name);

    constructor(
        private readonly ticketCreatedHandler: TicketCreatedHandler,
        private readonly ticketAssignedHandler: TicketAssignedHandler,
        private readonly ticketResolvedHandler: TicketResolvedHandler,
        private readonly ticketClosedHandler: TicketClosedHandler,
    ) {}

    dispatch(message: unknown): void {
        const event = message as TicketEventEnvelope;

        if (!event?.type || !event?.eventId || !event?.ticketId) {
            this.logger.warn('Received invalid ticket event envelope');
            return;
        }

        this.logger.log(
            `Consumed event from Kafka | type=${event.type} eventId=${event.eventId} ticketId=${event.ticketId}`,
        );

        switch (event.type as TicketEventType) {
            case TICKET_EVENT_TYPES.CREATED:
                this.ticketCreatedHandler.handle(event as TicketEventEnvelope<TicketCreatedPayload>);
                break;
            case TICKET_EVENT_TYPES.ASSIGNED:
                this.ticketAssignedHandler.handle(event as TicketEventEnvelope<TicketAssignedPayload>);
                break;
            case TICKET_EVENT_TYPES.RESOLVED:
                this.ticketResolvedHandler.handle(event as TicketEventEnvelope<TicketResolvedPayload>);
                break;
            case TICKET_EVENT_TYPES.CLOSED:
                this.ticketClosedHandler.handle(event as TicketEventEnvelope<TicketClosedPayload>);
                break;
            default:
                this.logger.warn(`No handler registered for event type: ${event.type}`);
        }
    }
}
