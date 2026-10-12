import { HttpStatus } from "@nestjs/common";
import { BaseVortexCommerceException } from "../../../common/exceptions/base-vortex-commerce-exception";

export class InvalidReservationQuantityException extends BaseVortexCommerceException {
  constructor(quantity: number) {
    super(
      `A quantidade a reservar deve ser um inteiro maior que zero. Recebido: ${quantity}.`,
      HttpStatus.BAD_REQUEST,
    );
  }
}
