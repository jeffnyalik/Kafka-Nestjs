import { KafkaModule } from "@app/kafka";
import { Module } from "@nestjs/common";

@Module({
    imports: [
        KafkaModule
    ]
})
export class AppModule {}