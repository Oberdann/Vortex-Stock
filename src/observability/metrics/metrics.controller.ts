import { Controller, Get, Res } from "@nestjs/common";
import { Response } from "express";
import { register } from "../../config/prometheus-config";

@Controller("metrics")
export class MetricsController {
  @Get()
  async getMetrics(@Res() response: Response) {
    response.set("Content-Type", register.contentType);
    response.end(await register.metrics());
  }
}
