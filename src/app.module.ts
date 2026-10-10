import { Module } from "@nestjs/common";
import { APP_FILTER } from "@nestjs/core";
import { LoggerModule } from "nestjs-pino";
import { ExceptionGlobalFilter } from "./common/filters/exception-global-filter";
import { loggerConfig } from "./config/pino-module-config";
import { DatabaseModule } from "./database/prisma.module";
import { MetricsModule } from "./observability/metrics/metrics.module";
import { StockModule } from "./modules/stock/stock.module";

@Module({
  imports: [
    LoggerModule.forRoot(loggerConfig),
    DatabaseModule,
    MetricsModule,
    StockModule,
  ],
  providers: [{ provide: APP_FILTER, useClass: ExceptionGlobalFilter }],
})
export class AppModule {}
