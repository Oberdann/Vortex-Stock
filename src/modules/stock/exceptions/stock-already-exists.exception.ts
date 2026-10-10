import { HttpStatus } from "@nestjs/common";
import { BaseVortexCommerceException } from "../../../common/exceptions/base-vortex-commerce-exception";

export class StockAlreadyExistsException extends BaseVortexCommerceException {
  constructor(productId: string) {
    super(
      `Já existe estoque cadastrado para o produto ${productId}.`,
      HttpStatus.CONFLICT,
    );
  }
}
