import {
  ArgumentsHost,
  Catch,
  ExceptionFilter,
  HttpException,
} from "@nestjs/common";
import { Response } from "express";
import { PinoLogger } from "nestjs-pino";

@Catch()
export class ExceptionGlobalFilter implements ExceptionFilter {
  constructor(private readonly logger: PinoLogger) {}

  catch(exception: unknown, host: ArgumentsHost) {
    const response = host.switchToHttp().getResponse<Response>();
    const request = host.switchToHttp().getRequest<Request>();
    const status =
      exception instanceof HttpException ? exception.getStatus() : 500;

    const body =
      exception instanceof HttpException ? exception.getResponse() : null;

    const message =
      typeof body === "string"
        ? body
        : ((body as { message?: string | string[] } | null)?.message ??
          "Erro interno do servidor.");

    this.logger.error(
      { method: request.method, url: request.url, status, err: exception },
      "Falha ao processar requisição de estoque.",
    );

    return response.status(status).json({ message, data: [], success: false });
  }
}
