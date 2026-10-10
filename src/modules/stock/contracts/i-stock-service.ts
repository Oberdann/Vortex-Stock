import { CreateStockDto } from "../dtos/create-stock.dto";
import { StockResponseDto } from "../dtos/stock-response.dto";
import { UpdateStockDto } from "../dtos/update-stock.dto";

export interface IStockService {
  create(stock: CreateStockDto): Promise<StockResponseDto>;
  getById(productId: string): Promise<StockResponseDto>;
  getByIds(productIds: string[]): Promise<StockResponseDto[]>;
  update(productId: string, stock: UpdateStockDto): Promise<StockResponseDto>;
}
