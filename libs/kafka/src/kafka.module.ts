import { DynamicModule, Module, Provider } from '@nestjs/common';
import { KafkaConsumerService } from './consumer/kafka.consumer.service';
import { KAFKA_MODULE_OPTIONS } from './constants/kafka.constants';
import { KafkaModuleOptions } from './interfaces/kafka-module-options.interface';
import { KafkaProducerService } from './producer/kafka.producer.service';

@Module({})
export class KafkaModule {
    static register(options: KafkaModuleOptions): DynamicModule {
        const providers: Provider[] = [
            {
                provide: KAFKA_MODULE_OPTIONS,
                useValue: options,
            },
            KafkaProducerService,
        ];
        const exports: (typeof KafkaProducerService | typeof KafkaConsumerService)[] = [
            KafkaProducerService,
        ];

        if (options.groupId) {
            providers.push(KafkaConsumerService);
            exports.push(KafkaConsumerService);
        }

        return {
            module: KafkaModule,
            providers,
            exports,
        };
    }
}
