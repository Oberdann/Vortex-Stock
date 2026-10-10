# Vortex Stock Service

Microsserviço responsável pela disponibilidade de estoque. Mantém banco PostgreSQL próprio e não consulta tabelas do Products Service.

## Endpoints iniciais

Todos usam o prefixo `/api/v1`.

- `POST /stock`: cria o estoque de um produto; `available` é opcional e inicia em `0`.
- `POST /stock/batch`: consulta em lote com `{ "productIds": ["<uuid>"] }`. IDs sem registro retornam disponibilidade zero.
- `GET /stock/:productId`: consulta o estoque de um produto.
- `DELETE /stock/:productId`: remove o registro.
- `GET /metrics`: métricas Prometheus.
- `/doc`: documentação Swagger.

O registro é identificado pelo `productId`, sem chave estrangeira para o banco do Products Service. Reservas e movimentações ainda não fazem parte deste esqueleto.

## Configuração

Configure `STOCK_DB_URL`, `PORT` (opcional, padrão `3000`) e `OTEL_EXPORTER_OTLP_ENDPOINT`. Use `npm run prisma:generate` para gerar o Prisma Client e `npm run prisma:migrate:dev` para aplicar a migration em desenvolvimento.

Configure também `RABBITMQ_URL`. O consumidor escuta `product.created`, cria o registro com disponibilidade zero e registra o `eventId` na Inbox para processamento idempotente. Falhas de processamento têm até cinco tentativas com intervalo de cinco segundos; mensagens inválidas ou esgotadas vão para a DLQ.

O Dockerfile aplica as migrations versionadas na inicialização.
