import { HttpStatus } from "@nestjs/common";
import { BaseVortexCommerceException } from "../../../common/exceptions/base-vortex-commerce-exception";

export class InsufficientStockQuantityException extends BaseVortexCommerceException {
  constructor(
    productId: string,
    requestedQuantity: number,
    availableQuantity: number,
  ) {
    super(
      `N\u00e3o h\u00e1 quantidade suficiente em estoque para o produto ${productId}. Solicitado: ${requestedQuantity}; dispon\u00edvel: ${availableQuantity}.`,
      HttpStatus.CONFLICT,
    );
  }
}
