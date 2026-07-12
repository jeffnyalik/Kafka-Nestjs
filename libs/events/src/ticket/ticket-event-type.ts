export const TICKET_EVENT_TYPES = {
    CREATED: 'TicketCreated',
    ASSIGNED: 'TicketAssigned',
    RESOLVED: 'TicketResolved',
    CLOSED: 'TicketClosed',
} as const;

export type TicketEventType = (typeof TICKET_EVENT_TYPES)[keyof typeof TICKET_EVENT_TYPES];
