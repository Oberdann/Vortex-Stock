import { Module } from "@nestjs/common";
import { DatabaseModule } from "src/database/prisma.module";
import { StockController } from "./stock.controller";
import { StockService } from "./stock.service";
import { ProductEventsConsumerService } from "src/modules/stock/consumers/product-events.consumer";
import { StockReservationConsumerService } from "src/modules/stock/consumers/stock-reservation.consumer";
import { OutboxPublisherService } from "src/modules/stock/producers/outbox-publisher.service";

@Module({
  imports: [DatabaseModule],
  controllers: [StockController],
  providers: [
    {
      provide: "IStockService",
      useClass: StockService,
    },
    ProductEventsConsumerService,
    StockReservationConsumerService,
    OutboxPublisherService,
  ],
})
export class StockModule {}
