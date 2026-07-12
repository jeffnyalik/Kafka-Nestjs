import { Injectable, Logger } from '@nestjs/common';
import { TicketClosedPayload, TicketEventEnvelope } from '@app/events';

@Injectable()
export class TicketClosedHandler {
    private readonly logger = new Logger(TicketClosedHandler.name);

    handle(event: TicketEventEnvelope<TicketClosedPayload>): void {
        this.logger.log(
            `Sending "ticket closed" notification | ticketId=${event.ticketId} ` +
            `closedBy=${event.payload.closedBy} reason="${event.payload.reason ?? 'n/a'}"`,
        );
    }
}
