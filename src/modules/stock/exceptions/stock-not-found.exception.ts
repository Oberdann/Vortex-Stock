import { HttpStatus } from "@nestjs/common";
import { BaseVortexCommerceException } from "../../../common/exceptions/base-vortex-commerce-exception";

export class StockNotFoundException extends BaseVortexCommerceException {
  constructor(productId: string) {
    super(
      `Estoque do produto ${productId} não encontrado.`,
      HttpStatus.NOT_FOUND,
    );
  }
}
