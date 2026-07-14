import { TicketCreatedPayload, TicketEventEnvelope } from '@app/events';
import { KAFKA_TOPICS, KafkaConsumerService } from '@app/kafka';
import { Injectable, Logger, OnApplicationBootstrap } from '@nestjs/common';
import { TicketEventDispatcher } from 'apps/notification-service/src/handlers/ticket-event.dispatcher';

@Injectable()
export class AnalyticsServiceService implements OnApplicationBootstrap {
  private readonly logger = new Logger(AnalyticsServiceService.name);
  private ticketsCreated = 0;
  constructor(
    private readonly kafkaConsumerService:  KafkaConsumerService,
    private readonly ticketEventDispatcher: TicketEventDispatcher
  ){}

  async onApplicationBootstrap(): Promise<void> {
    await this.kafkaConsumerService.subscribe(
      KAFKA_TOPICS.TICKET_EVENTS, async (message: unknown) => this.ticketEventDispatcher.dispatch(message) )
      this.ticketsCreated++;
      this.logger.log(`Listening for ticket events on topic "${KAFKA_TOPICS.TICKET_EVENTS}"`);
  }
}
