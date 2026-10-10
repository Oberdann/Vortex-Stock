import { ApiProperty, ApiPropertyOptional } from "@nestjs/swagger";
import { IsInt, IsOptional, IsUUID, Min } from "class-validator";

export class CreateStockDto {
  @ApiProperty({ description: "ID do produto no Products Service" })
  @IsUUID("4", { message: "O ID do produto deve ser um UUID válido." })
  productId: string;

  @ApiPropertyOptional({ default: 0, minimum: 0 })
  @IsOptional()
  @IsInt({ message: "A quantidade disponível deve ser um número inteiro." })
  @Min(0, { message: "A quantidade disponível não pode ser negativa." })
  available?: number;
}
