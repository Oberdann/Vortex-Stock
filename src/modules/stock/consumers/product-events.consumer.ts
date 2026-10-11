import { Injectable, OnModuleDestroy, OnModuleInit } from "@nestjs/common";
import * as amqp from "amqplib";
import { ConfirmChannel, ConsumeMessage } from "amqplib";
import { PinoLogger } from "nestjs-pino";
import { PrismaService } from "src/database/prisma.service";
import {
  MAX_PROCESSING_RETRIES,
  PRODUCT_CREATED_EVENT,
  PRODUCT_DELETED_EVENT,
  RABBITMQ_EVENTS_EXCHANGE,
  RETRY_DELAY_MS,
  STOCK_PRODUCT_CREATED_DLQ,
  STOCK_PRODUCT_CREATED_DLX,
  STOCK_PRODUCT_CREATED_FAILED_ROUTE,
  STOCK_PRODUCT_CREATED_QUEUE,
  STOCK_PRODUCT_CREATED_RETRY_QUEUE,
  STOCK_PRODUCT_DELETED_DLQ,
  STOCK_PRODUCT_DELETED_FAILED_ROUTE,
  STOCK_PRODUCT_DELETED_QUEUE,
  STOCK_PRODUCT_DELETED_RETRY_QUEUE,
} from "src/common/messaging/rabbitmq-topology";

interface ProductEvent {
  eventId: string;
  eventType: typeof PRODUCT_CREATED_EVENT | typeof PRODUCT_DELETED_EVENT;
  data: { productId: string };
}

const UUID_PATTERN =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
const RECONNECT_DELAY_MS = 5000;

@Injectable()
export class ProductEventsConsumerService
  implements OnModuleInit, OnModuleDestroy
{
  private connection?: amqp.ChannelModel;
  private channel?: ConfirmChannel;
  private consumerTags: string[] = [];
  private reconnectTimer?: NodeJS.Timeout;
  private shuttingDown = false;

  constructor(
    private readonly prisma: PrismaService,
    private readonly logger: PinoLogger,
  ) {}

  onModuleInit() {
    void this.connectAndConsume();
  }

  async onModuleDestroy() {
    this.shuttingDown = true;
    if (this.reconnectTimer) clearTimeout(this.reconnectTimer);

    if (this.channel) {
      await Promise.all(
        this.consumerTags.map((consumerTag) =>
          this.channel?.cancel(consumerTag).catch(() => undefined),
        ),
      );
    }

    await this.connection?.close().catch(() => undefined);
  }

  private async connectAndConsume() {
    if (this.shuttingDown || this.connection) return;

    const url = process.env.RABBITMQ_URL;
    if (!url) {
      this.logger.error("RABBITMQ_URL não foi configurada no Stock Service.");
      this.scheduleReconnect();
      return;
    }

    try {
      const connection = await amqp.connect(url);
      this.connection = connection;
      connection.on("error", () => undefined);
      connection.on("close", () => this.handleDisconnect());

      const channel = await connection.createConfirmChannel();
      this.channel = channel;
      channel.on("error", () => undefined);

      await this.declareTopology(channel);
      await channel.prefetch(10);

      const createdConsumer = await channel.consume(
        STOCK_PRODUCT_CREATED_QUEUE,
        (message) => {
          if (message) void this.handleMessage(channel, message);
        },
        { noAck: false },
      );
      this.consumerTags.push(createdConsumer.consumerTag);

      const deletedConsumer = await channel.consume(
        STOCK_PRODUCT_DELETED_QUEUE,
        (message) => {
          if (message) void this.handleMessage(channel, message);
        },
        { noAck: false },
      );
      this.consumerTags.push(deletedConsumer.consumerTag);

      this.logger.info(
        { queues: [STOCK_PRODUCT_CREATED_QUEUE, STOCK_PRODUCT_DELETED_QUEUE] },
        "Consumidor de eventos de produto conectado.",
      );
    } catch (error) {
      this.logger.error(
        { errorType: error instanceof Error ? error.name : "UnknownError" },
        "Falha ao conectar o consumidor de eventos de produto.",
      );

      await this.closeConnection();

      this.scheduleReconnect();
    }
  }

  private async declareTopology(channel: ConfirmChannel) {
    await channel.assertExchange(RABBITMQ_EVENTS_EXCHANGE, "topic", {
      durable: true,
    });

    await channel.assertExchange(STOCK_PRODUCT_CREATED_DLX, "topic", {
      durable: true,
    });

    await channel.assertQueue(STOCK_PRODUCT_CREATED_DLQ, { durable: true });

    await channel.bindQueue(
      STOCK_PRODUCT_CREATED_DLQ,
      STOCK_PRODUCT_CREATED_DLX,
      STOCK_PRODUCT_CREATED_FAILED_ROUTE,
    );

    await channel.assertQueue(STOCK_PRODUCT_CREATED_RETRY_QUEUE, {
      durable: true,
      arguments: {
        "x-message-ttl": RETRY_DELAY_MS,
        "x-dead-letter-exchange": RABBITMQ_EVENTS_EXCHANGE,
        "x-dead-letter-routing-key": PRODUCT_CREATED_EVENT,
      },
    });

    await channel.assertQueue(STOCK_PRODUCT_CREATED_QUEUE, {
      durable: true,
      arguments: {
        "x-dead-letter-exchange": STOCK_PRODUCT_CREATED_DLX,
        "x-dead-letter-routing-key": STOCK_PRODUCT_CREATED_FAILED_ROUTE,
      },
    });

    await channel.bindQueue(
      STOCK_PRODUCT_CREATED_QUEUE,
      RABBITMQ_EVENTS_EXCHANGE,
      PRODUCT_CREATED_EVENT,
    );

    await channel.assertQueue(STOCK_PRODUCT_DELETED_DLQ, { durable: true });

    await channel.bindQueue(
      STOCK_PRODUCT_DELETED_DLQ,
      STOCK_PRODUCT_CREATED_DLX,
      STOCK_PRODUCT_DELETED_FAILED_ROUTE,
    );

    await channel.assertQueue(STOCK_PRODUCT_DELETED_RETRY_QUEUE, {
      durable: true,
      arguments: {
        "x-message-ttl": RETRY_DELAY_MS,
        "x-dead-letter-exchange": RABBITMQ_EVENTS_EXCHANGE,
        "x-dead-letter-routing-key": PRODUCT_DELETED_EVENT,
      },
    });

    await channel.assertQueue(STOCK_PRODUCT_DELETED_QUEUE, {
      durable: true,
      arguments: {
        "x-dead-letter-exchange": STOCK_PRODUCT_CREATED_DLX,
        "x-dead-letter-routing-key": STOCK_PRODUCT_DELETED_FAILED_ROUTE,
      },
    });

    await channel.bindQueue(
      STOCK_PRODUCT_DELETED_QUEUE,
      RABBITMQ_EVENTS_EXCHANGE,
      PRODUCT_DELETED_EVENT,
    );
  }

  private async handleMessage(
    channel: ConfirmChannel,
    message: ConsumeMessage,
  ) {
    const event = this.parseEvent(message);

    if (!event) {
      this.logger.warn(
        { messageId: message.properties.messageId },
        "Evento de produto inválido enviado para a DLQ.",
      );

      channel.nack(message, false, false);

      return;
    }

    try {
      await this.prisma.$transaction(
        async (transaction) => {
          const processed = await transaction.inboxEvent.findUnique({
            where: { eventId: event.eventId },
          });

          if (processed) return;

          const productWasDeleted =
            event.eventType === PRODUCT_CREATED_EVENT &&
            (await transaction.inboxEvent.findFirst({
              where: {
                aggregateId: event.data.productId,
                eventType: PRODUCT_DELETED_EVENT,
              },
            }));

          await transaction.inboxEvent.create({
            data: {
              eventId: event.eventId,
              eventType: event.eventType,
              aggregateId: event.data.productId,
            },
          });

          if (event.eventType === PRODUCT_CREATED_EVENT) {
            if (!productWasDeleted) {
              await transaction.stock.upsert({
                where: { productId: event.data.productId },
                create: { productId: event.data.productId, available: 0 },
                update: {},
              });
            }
          } else {
            await transaction.stock.deleteMany({
              where: { productId: event.data.productId },
            });
          }
        },
        { isolationLevel: "Serializable" },
      );

      channel.ack(message);
    } catch (error) {
      if (this.isUniqueConstraintError(error)) {
        channel.ack(message);

        return;
      }

      await this.retryOrDeadLetter(channel, message, event.eventType, error);
    }
  }

  private parseEvent(message: ConsumeMessage): ProductEvent | undefined {
    try {
      const event = JSON.parse(
        message.content.toString(),
      ) as Partial<ProductEvent>;

      if (
        (event.eventType !== PRODUCT_CREATED_EVENT &&
          event.eventType !== PRODUCT_DELETED_EVENT) ||
        !event.eventId ||
        !UUID_PATTERN.test(event.eventId) ||
        !event.data?.productId ||
        !UUID_PATTERN.test(event.data.productId)
      ) {
        return undefined;
      }

      return event as ProductEvent;
    } catch {
      return undefined;
    }
  }

  private async retryOrDeadLetter(
    channel: ConfirmChannel,
    message: ConsumeMessage,
    eventType: ProductEvent["eventType"],
    error: unknown,
  ) {
    const retryHeader = message.properties.headers?.retryCount;

    const retries =
      typeof retryHeader === "number" && Number.isInteger(retryHeader)
        ? retryHeader
        : 0;

    if (retries >= MAX_PROCESSING_RETRIES) {
      this.logger.error(
        {
          messageId: message.properties.messageId,
          retries,
          errorType: error instanceof Error ? error.name : "UnknownError",
        },
        "Evento excedeu as tentativas e foi enviado para a DLQ.",
      );

      channel.nack(message, false, false);

      return;
    }

    try {
      const retryQueue =
        eventType === PRODUCT_CREATED_EVENT
          ? STOCK_PRODUCT_CREATED_RETRY_QUEUE
          : STOCK_PRODUCT_DELETED_RETRY_QUEUE;

      channel.sendToQueue(retryQueue, message.content, {
        persistent: true,
        contentType: message.properties.contentType,
        messageId: message.properties.messageId,
        headers: { ...message.properties.headers, retryCount: retries + 1 },
      });

      await channel.waitForConfirms();

      channel.ack(message);
    } catch (publishError) {
      this.logger.error(
        {
          messageId: message.properties.messageId,
          errorType:
            publishError instanceof Error ? publishError.name : "UnknownError",
        },
        "Falha ao agendar retry; evento enviado para a DLQ.",
      );
      channel.nack(message, false, false);
    }
  }

  private isUniqueConstraintError(error: unknown) {
    return (
      typeof error === "object" &&
      error !== null &&
      "code" in error &&
      error.code === "P2002"
    );
  }

  private handleDisconnect() {
    this.connection = undefined;
    this.channel = undefined;
    this.consumerTags = [];

    if (!this.shuttingDown) {
      this.logger.warn("Conexão com RabbitMQ perdida; reconexão agendada.");
      this.scheduleReconnect();
    }
  }

  private scheduleReconnect() {
    if (this.shuttingDown || this.reconnectTimer) return;

    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = undefined;
      void this.connectAndConsume();
    }, RECONNECT_DELAY_MS);

    this.reconnectTimer.unref();
  }

  private async closeConnection() {
    const connection = this.connection;
    this.connection = undefined;
    this.channel = undefined;
    this.consumerTags = [];
    await connection?.close().catch(() => undefined);
  }
}
