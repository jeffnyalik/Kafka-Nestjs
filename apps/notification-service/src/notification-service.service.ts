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
                this.logger.log(
                    `Consumed event from Kafka [topic=${KAFKA_TOPICS.TICKET_EVENTS}] ` +
                    `eventId=${event.eventId} ticketId=${event.ticketId} ` +
                    `title="${event.title}" priority=${event.priority ?? 'n/a'} ` +
                    `occurredAt=${event.occurredAt}`,
                );
            },
        );

        this.logger.log(`Listening for events on topic "${KAFKA_TOPICS.TICKET_EVENTS}"`);
    }

    getHello(): string {
        return 'Notification service is running';
    }
}
