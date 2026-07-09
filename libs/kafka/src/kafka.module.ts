import { Module } from '@nestjs/common';
import { KafkaProducerService } from './producer/kafka.producer.service';

@Module({
  providers: [KafkaProducerService],
  exports: [KafkaProducerService],
})
export class KafkaModule {
  constructor(private readonly kafkaProducer: KafkaProducerService) {}
}
