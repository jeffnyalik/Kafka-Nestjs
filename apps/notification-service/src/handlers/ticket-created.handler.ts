import { Injectable, Logger } from '@nestjs/common';
import { TicketCreatedPayload, TicketEventEnvelope } from '@app/events';

@Injectable()
export class TicketCreatedHandler {
    private readonly logger = new Logger(TicketCreatedHandler.name);

    handle(event: TicketEventEnvelope<TicketCreatedPayload>): void {
        this.logger.log(
            `Sending "ticket created" notification | ticketId=${event.ticketId} ` +
            `title="${event.payload.title}" priority=${event.payload.priority ?? 'n/a'}`,
        );
    }
}
