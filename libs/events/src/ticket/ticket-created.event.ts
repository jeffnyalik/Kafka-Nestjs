export class TicketCreatedEvent{
    eventId: string; //unique identifier for the event
    ticketId: string; //identify the business entity

    title: string;
    description: string;
    priority: string;
    occurredAt: Date; //tells the consumer when the event occured.
}