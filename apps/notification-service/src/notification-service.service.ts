import { KAFKA_TOPICS, KafkaConsumerService } from '@app/kafka';
import { Injectable, Logger, OnApplicationBootstrap } from '@nestjs/common';
import { TicketEventDispatcher } from './handlers/ticket-event.dispatcher';

@Injectable()
export class NotificationServiceService implements OnApplicationBootstrap {
    private readonly logger = new Logger(NotificationServiceService.name);

    constructor(
        private readonly kafkaConsumerService: KafkaConsumerService,
        private readonly ticketEventDispatcher: TicketEventDispatcher,
    ) {}

    async onApplicationBootstrap() {
        await this.kafkaConsumerService.subscribe(
            KAFKA_TOPICS.TICKET_EVENTS,
            async (message) => this.ticketEventDispatcher.dispatch(message),
        );

        this.logger.log(`Listening for ticket events on topic "${KAFKA_TOPICS.TICKET_EVENTS}"`);
    }

    getHello(): string {
        return 'Notification service is running';
    }
}
