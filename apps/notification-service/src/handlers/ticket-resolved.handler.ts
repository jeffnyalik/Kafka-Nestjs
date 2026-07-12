import { Injectable, Logger } from '@nestjs/common';
import { TicketEventEnvelope, TicketResolvedPayload } from '@app/events';

@Injectable()
export class TicketResolvedHandler {
    private readonly logger = new Logger(TicketResolvedHandler.name);

    handle(event: TicketEventEnvelope<TicketResolvedPayload>): void {
        this.logger.log(
            `Sending "ticket resolved" notification | ticketId=${event.ticketId} ` +
            `resolvedBy=${event.payload.resolvedBy} resolution="${event.payload.resolution}"`,
        );
    }
}
