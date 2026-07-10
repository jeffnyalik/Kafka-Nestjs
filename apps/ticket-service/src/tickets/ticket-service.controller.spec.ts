import { Test, TestingModule } from '@nestjs/testing';
import { TicketServiceController } from './ticket-service.controller';
import { TicketServiceService } from './ticket-service.service';

describe('TicketServiceController', () => {
  let ticketServiceController: TicketServiceController;

  beforeEach(async () => {
    const app: TestingModule = await Test.createTestingModule({
      controllers: [TicketServiceController],
      providers: [TicketServiceService],
    }).compile();

    ticketServiceController = app.get<TicketServiceController>(TicketServiceController);
  });

  describe('root', () => {
    it('should return "Hello World!"', () => {
      expect(ticketServiceController.getHello()).toBe('Hello World!');
    });
  });
});
