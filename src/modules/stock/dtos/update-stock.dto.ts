import { ApiProperty } from "@nestjs/swagger";
import { IsInt, Min } from "class-validator";

export class UpdateStockDto {
  @ApiProperty({ minimum: 0 })
  @IsInt({ message: "A quantidade disponível deve ser um número inteiro." })
  @Min(0, { message: "A quantidade disponível não pode ser negativa." })
  available: number;
}
