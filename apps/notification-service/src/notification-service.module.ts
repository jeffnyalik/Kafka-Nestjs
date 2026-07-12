import { Module } from '@nestjs/common';
import { KafkaModule } from '@app/kafka';
import { TicketAssignedHandler } from './handlers/ticket-assigned.handler';
import { TicketClosedHandler } from './handlers/ticket-closed.handler';
import { TicketCreatedHandler } from './handlers/ticket-created.handler';
import { TicketEventDispatcher } from './handlers/ticket-event.dispatcher';
import { TicketResolvedHandler } from './handlers/ticket-resolved.handler';
import { NotificationServiceController } from './notification-service.controller';
import { NotificationServiceService } from './notification-service.service';

@Module({
    imports: [
        KafkaModule.register({
            clientId: 'notification-service',
            brokers: ['localhost:9092'],
            groupId: 'notification-group',
        }),
    ],
    controllers: [NotificationServiceController],
    providers: [
        NotificationServiceService,
        TicketEventDispatcher,
        TicketCreatedHandler,
        TicketAssignedHandler,
        TicketResolvedHandler,
        TicketClosedHandler,
    ],
})
export class NotificationServiceModule {}
