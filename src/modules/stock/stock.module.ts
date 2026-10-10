import { Module } from "@nestjs/common";
import { DatabaseModule } from "src/database/prisma.module";
import { StockController } from "./stock.controller";
import { StockService } from "./stock.service";
import { ProductCreatedConsumerService } from "src/modules/stock/consumers/product-created.consumer";

@Module({
  imports: [DatabaseModule],
  controllers: [StockController],
  providers: [
    {
      provide: "IStockService",
      useClass: StockService,
    },
    ProductCreatedConsumerService,
  ],
})
export class StockModule {}
