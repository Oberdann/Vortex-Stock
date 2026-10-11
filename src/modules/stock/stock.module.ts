import { Module } from "@nestjs/common";
import { DatabaseModule } from "src/database/prisma.module";
import { StockController } from "./stock.controller";
import { StockService } from "./stock.service";
import { ProductEventsConsumerService } from "src/modules/stock/consumers/product-events.consumer";

@Module({
  imports: [DatabaseModule],
  controllers: [StockController],
  providers: [
    {
      provide: "IStockService",
      useClass: StockService,
    },
    ProductEventsConsumerService,
  ],
})
export class StockModule {}
