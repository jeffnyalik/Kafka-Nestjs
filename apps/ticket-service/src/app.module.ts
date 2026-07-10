import { KafkaModule } from "@app/kafka";
import { Module } from "@nestjs/common";
import { TicketServiceService } from "./ticket-service.service";

@Module({
    imports: [
        KafkaModule
    ],
    providers: [TicketServiceService]
})
export class AppModule {}