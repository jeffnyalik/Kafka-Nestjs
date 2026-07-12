import { Injectable, Logger } from '@nestjs/common';
import { TicketAssignedPayload, TicketEventEnvelope } from '@app/events';

@Injectable()
export class TicketAssignedHandler {
    private readonly logger = new Logger(TicketAssignedHandler.name);

    handle(event: TicketEventEnvelope<TicketAssignedPayload>): void {
        this.logger.log(
            `Sending "ticket assigned" notification | ticketId=${event.ticketId} ` +
            `assignee=${event.payload.assigneeEmail} assignedBy=${event.payload.assignedBy}`,
        );
    }
}
