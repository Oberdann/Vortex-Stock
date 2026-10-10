import { Prisma, Stock } from "generated/prisma/client";
import { CreateStockDto } from "../dtos/create-stock.dto";
import { StockResponseDto } from "../dtos/stock-response.dto";

export class StockMapper {
  static toResponseDto(stock: Stock): StockResponseDto {
    return {
      productId: stock.productId,
      available: stock.available,
      inStock: stock.available > 0,
    };
  }

  static toBatchResponseDto(
    productIds: string[],
    stocks: Stock[],
  ): StockResponseDto[] {
    const stocksByProductId = new Map(
      stocks.map((stock) => [stock.productId, stock]),
    );

    return productIds.map((productId) => {
      const stock = stocksByProductId.get(productId);

      return stock
        ? StockMapper.toResponseDto(stock)
        : { productId, available: 0, inStock: false };
    });
  }

  static toPrismaCreate(dto: CreateStockDto): Prisma.StockCreateInput {
    return {
      productId: dto.productId,
      available: dto.available ?? 0,
    };
  }
}
