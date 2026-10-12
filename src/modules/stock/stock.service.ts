import { Injectable } from "@nestjs/common";
import { PrismaService } from "src/database/prisma.service";
import { IStockService } from "./contracts/i-stock-service";
import { CreateStockDto } from "./dtos/create-stock.dto";
import { StockResponseDto } from "./dtos/stock-response.dto";
import { StockAlreadyExistsException } from "./exceptions/stock-already-exists.exception";
import { StockNotFoundException } from "./exceptions/stock-not-found.exception";
import { InsufficientStockQuantityException } from "./exceptions/insufficient-stock-quantity.exception";
import { StockMapper } from "./mapper/stock.mapper";
import { UpdateStockDto } from "./dtos/update-stock.dto";
import { InvalidReservationQuantityException } from "./exceptions/invalid-reservation-quantity.exception";

@Injectable()
export class StockService implements IStockService {
  constructor(private readonly prisma: PrismaService) { }

  async create(stock: CreateStockDto): Promise<StockResponseDto> {
    const existingStock = await this.prisma.stock.findUnique({
      where: { productId: stock.productId },
    });

    if (existingStock) {
      throw new StockAlreadyExistsException(stock.productId);
    }

    const createdStock = await this.prisma.stock.create({
      data: StockMapper.toPrismaCreate(stock),
    });

    const response = StockMapper.toResponseDto(createdStock);

    return response;
  }

  async getById(productId: string): Promise<StockResponseDto> {
    const stock = await this.findStockOrFail(productId);

    const response = StockMapper.toResponseDto(stock);

    return response;
  }

  async getByIds(productIds: string[]): Promise<StockResponseDto[]> {
    const stocks = await this.prisma.stock.findMany({
      where: { productId: { in: productIds } },
    });

    const response = StockMapper.toBatchResponseDto(productIds, stocks);

    return response;
  }

  async update(
    productId: string,
    stock: UpdateStockDto,
  ): Promise<StockResponseDto> {
    await this.findStockOrFail(productId);

    const updatedStock = await this.prisma.stock.update({
      where: { productId },
      data: { available: stock.available },
    });

    const response = StockMapper.toResponseDto(updatedStock);

    return response;
  }

  async reserveQuantity(productId: string, quantity: number): Promise<void> {
    const stock = await this.findStockOrFail(productId);

    const quantityNotIsNumberValid = !Number.isInteger(quantity) || quantity <= 0;

    if (quantityNotIsNumberValid) {
      throw new InvalidReservationQuantityException(quantity);
    }

    if (stock.available < quantity) {
      throw new InsufficientStockQuantityException(productId, quantity, stock.available);
    }

    await this.prisma.stock.update({
      where: { productId },
      data: {
        available: stock.available - quantity,
        reserved: stock.reserved + quantity,
      },
    });
  }

  private async findStockOrFail(productId: string) {
    const stock = await this.prisma.stock.findUnique({
      where: { productId },
    });

    if (!stock) {
      throw new StockNotFoundException(productId);
    }

    return stock;
  }
}
