import {
  Body,
  Controller,
  Get,
  HttpCode,
  Inject,
  Param,
  Post,
  Put,
} from "@nestjs/common";
import { Ok } from "src/common/utils/response-util";
import { IStockService } from "./contracts/i-stock-service";
import { CreateStockDto } from "./dtos/create-stock.dto";
import { StockBatchDto } from "./dtos/stock-batch.dto";
import { UpdateStockDto } from "./dtos/update-stock.dto";

@Controller("stock")
export class StockController {
  constructor(
    @Inject("IStockService")
    private readonly stockService: IStockService,
  ) {}

  @HttpCode(201)
  @Post()
  async create(@Body() stock: CreateStockDto) {
    const response = await this.stockService.create(stock);

    return Ok("Estoque criado com sucesso.", response);
  }

  @HttpCode(200)
  @Post("batch")
  async getBatch(@Body() stockIds: StockBatchDto) {
    const response = await this.stockService.getByIds(stockIds.productIds);

    return Ok("Estoques consultados com sucesso.", response);
  }

  @HttpCode(200)
  @Get(":productId")
  async getById(@Param("productId") productId: string) {
    const response = await this.stockService.getById(productId);

    return Ok("Estoque encontrado com sucesso.", response);
  }

  @HttpCode(200)
  @Put(":productId")
  async update(
    @Param("productId") productId: string,
    @Body() stock: UpdateStockDto,
  ) {
    const response = await this.stockService.update(productId, stock);

    return Ok("Estoque atualizado com sucesso.", response);
  }
}
