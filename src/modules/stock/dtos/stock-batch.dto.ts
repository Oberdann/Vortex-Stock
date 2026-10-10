import { ApiProperty } from "@nestjs/swagger";
import { ArrayNotEmpty, IsArray, IsUUID } from "class-validator";

export class StockBatchDto {
  @ApiProperty({ type: [String], description: "IDs dos produtos" })
  @IsArray({ message: "productIds deve ser uma lista." })
  @ArrayNotEmpty({ message: "Informe pelo menos um ID de produto." })
  @IsUUID("4", {
    each: true,
    message: "Cada ID de produto deve ser um UUID válido.",
  })
  productIds: string[];
}
