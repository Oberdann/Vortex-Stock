import { HttpException, HttpStatus } from "@nestjs/common";

export class BaseVortexCommerceException extends HttpException {
  constructor(
    message = "Erro interno do serviço de estoque.",
    statusCode = HttpStatus.INTERNAL_SERVER_ERROR,
  ) {
    super(message, statusCode);
  }
}
