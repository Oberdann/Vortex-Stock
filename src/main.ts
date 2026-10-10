import * as dotenv from "dotenv";
dotenv.config();

import "./observability/instrumentation";

import { ValidationPipe } from "@nestjs/common";
import { NestFactory } from "@nestjs/core";
import { DocumentBuilder, SwaggerModule } from "@nestjs/swagger";
import { AppModule } from "./app.module";
import { MetricsInterceptor } from "./common/interceptors/metrics-interceptor";

async function bootstrap() {
  const app = await NestFactory.create(AppModule);

  app.setGlobalPrefix("api/v1");

  app.useGlobalPipes(
    new ValidationPipe({
      whitelist: true,
      forbidNonWhitelisted: true,
      transform: true,
    }),
  );

  app.useGlobalInterceptors(new MetricsInterceptor());

  const config = new DocumentBuilder()
    .setTitle("Vortex API - Stock")
    .setDescription("Microsserviço responsável pelo estoque dos produtos")
    .setVersion("0.0.1")
    .build();

  SwaggerModule.setup("doc", app, SwaggerModule.createDocument(app, config));

  await app.listen(process.env.PORT ?? 3000);
}

void bootstrap();
