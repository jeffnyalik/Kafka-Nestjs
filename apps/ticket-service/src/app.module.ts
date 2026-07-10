import { KafkaModule } from "@app/kafka";
import { Module } from "@nestjs/common";
import { TicketServiceService } from "./tickets/ticket-service.service";
import { TicketServiceController } from "./tickets/ticket-service.controller";

@Module({
    imports: [
        KafkaModule
    ],
    controllers: [TicketServiceController],
    providers: [TicketServiceService]
})
export class AppModule {}