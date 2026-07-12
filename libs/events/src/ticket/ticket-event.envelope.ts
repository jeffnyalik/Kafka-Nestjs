import { TicketEventType } from './ticket-event-type';

export interface TicketEventEnvelope<TPayload = unknown> {
    type: TicketEventType;
    eventId: string;
    ticketId: string;
    occurredAt: Date;
    payload: TPayload;
}
