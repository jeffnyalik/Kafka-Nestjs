import { KAFKA_TOPICS, KafkaConsumerService } from '@app/kafka';
import { Injectable, Logger, OnApplicationBootstrap } from '@nestjs/common';
import { TicketCreatedEvent } from '@app/events/ticket/ticket-created.event';

@Injectable()
export class NotificationServiceService implements OnApplicationBootstrap {
    private readonly logger = new Logger(NotificationServiceService.name);

    constructor(
        private readonly kafkaConsumerService: KafkaConsumerService,
    ) {}

    async onApplicationBootstrap() {
        await this.kafkaConsumerService.subscribe(
            KAFKA_TOPICS.TICKET_EVENTS,
            async (message: unknown) => {
                const event = message as TicketCreatedEvent;
                this.logger.log(`Ticket created: ${event.ticketId} - ${event.title}`);
            },
        );
    }

    getHello(): string {
        return 'Notification service is running';
    }
}
