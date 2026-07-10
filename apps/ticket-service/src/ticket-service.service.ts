import { KafkaProducerService } from '@app/kafka';
import { Injectable } from '@nestjs/common';

@Injectable()
export class TicketServiceService {
  constructor(private readonly kafkaProducer: KafkaProducerService){}
  getHello(): string {
    return 'Hello World!';
  }
}
