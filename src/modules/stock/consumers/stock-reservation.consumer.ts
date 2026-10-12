import { Injectable, OnModuleDestroy, OnModuleInit } from "@nestjs/common";
import * as amqp from "amqplib";
import { ConfirmChannel, ConsumeMessage } from "amqplib";
import { PinoLogger } from "nestjs-pino";
import { PrismaService } from "src/database/prisma.service";
import {
  MAX_PROCESSING_RETRIES,
  RABBITMQ_EVENTS_EXCHANGE,
  RETRY_DELAY_MS,
  STOCK_RESERVATION_CONFIRMED_EVENT,
  STOCK_RESERVATION_DLQ,
  STOCK_RESERVATION_FAILED_ROUTE,
  STOCK_RESERVATION_QUEUE,
  STOCK_RESERVATION_REJECTED_EVENT,
  STOCK_RESERVATION_REQUESTED_EVENT,
  STOCK_RESERVATION_RETRY_QUEUE,
} from "src/common/messaging/rabbitmq-topology";

interface ReservationItem {
  productId: string;
  quantity: number;
}

interface ReservationRequestedEvent {
  eventId: string;
  eventType: typeof STOCK_RESERVATION_REQUESTED_EVENT;
  data: { orderId: string; items: ReservationItem[] };
}

class ReservationRejectedError extends Error {
  constructor(
    readonly productId: string,
    readonly requestedQuantity: number,
    readonly availableQuantity: number,
    readonly reason: "STOCK_NOT_FOUND" | "INSUFFICIENT_STOCK",
  ) {
    super(reason);
  }
}

const UUID_PATTERN =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
const RECONNECT_DELAY_MS = 5000;
const PREFETCH_COUNT = 10;

@Injectable()
export class StockReservationConsumerService
  implements OnModuleInit, OnModuleDestroy
{
  private connection?: amqp.ChannelModel;
  private channel?: ConfirmChannel;
  private consumerTag?: string;
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

    if (this.consumerTag) {
      await this.channel?.cancel(this.consumerTag).catch(() => undefined);
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
      await channel.prefetch(PREFETCH_COUNT);

      const consumer = await channel.consume(
        STOCK_RESERVATION_QUEUE,
        (message) => {
          if (message) void this.handleMessage(channel, message);
        },
        { noAck: false },
      );
      this.consumerTag = consumer.consumerTag;

      this.logger.info(
        { queue: STOCK_RESERVATION_QUEUE },
        "Consumidor de solicitações de reserva conectado.",
      );

    } catch (error) {

      this.logger.error(
        { errorType: error instanceof Error ? error.name : "UnknownError" },
        "Falha ao conectar o consumidor de solicitações de reserva.",
      );

      await this.closeConnection();

      this.scheduleReconnect();
    }
  }

  private async declareTopology(channel: ConfirmChannel) {
    await channel.assertExchange(RABBITMQ_EVENTS_EXCHANGE, "topic", {
      durable: true,
    });

    await channel.assertExchange("vortex.events.dlx", "topic", {
      durable: true,
    });

    await channel.assertQueue(STOCK_RESERVATION_DLQ, { durable: true });

    await channel.bindQueue(
      STOCK_RESERVATION_DLQ,
      "vortex.events.dlx",
      STOCK_RESERVATION_FAILED_ROUTE,
    );

    await channel.assertQueue(STOCK_RESERVATION_RETRY_QUEUE, {
      durable: true,
      arguments: {
        "x-message-ttl": RETRY_DELAY_MS,
        "x-dead-letter-exchange": RABBITMQ_EVENTS_EXCHANGE,
        "x-dead-letter-routing-key": STOCK_RESERVATION_REQUESTED_EVENT,
      },
    });

    await channel.assertQueue(STOCK_RESERVATION_QUEUE, {
      durable: true,
      arguments: {
        "x-dead-letter-exchange": "vortex.events.dlx",
        "x-dead-letter-routing-key": STOCK_RESERVATION_FAILED_ROUTE,
      },
    });

    await channel.bindQueue(
      STOCK_RESERVATION_QUEUE,
      RABBITMQ_EVENTS_EXCHANGE,
      STOCK_RESERVATION_REQUESTED_EVENT,
    );
  }

  private async handleMessage(channel: ConfirmChannel, message: ConsumeMessage) {
    const event = this.parseEvent(message);
    if (!event) {
      this.logger.warn(
        { messageId: message.properties.messageId },
        "Solicitação de reserva inválida enviada para a DLQ.",
      );
      channel.nack(message, false, false);
      return;
    }

    try {
      await this.processEvent(event);

      channel.ack(message);
    } catch (error) {
      if (this.isUniqueConstraintError(error)) {
        channel.ack(message);
        return;
      }

      await this.retryOrDeadLetter(channel, message, error);
    }
  }

  private async processEvent(event: ReservationRequestedEvent) {
    const items = this.aggregateItems(event.data.items);
    try {
      await this.prisma.$transaction(
        async (transaction) => {
          const alreadyProcessed = await transaction.inboxEvent.findUnique({
            where: { eventId: event.eventId },
          });

          if (alreadyProcessed) return;

          for (const item of items) {
            const result = await transaction.stock.updateMany({
              where: {
                productId: item.productId,
                available: { gte: item.quantity },
              },
              data: {
                available: { decrement: item.quantity },
                reserved: { increment: item.quantity },
              },
            });

            if (result.count === 0) {
              const stock = await transaction.stock.findUnique({
                where: { productId: item.productId },
              });

              throw new ReservationRejectedError(
                item.productId,
                item.quantity,
                stock?.available ?? 0,
                stock ? "INSUFFICIENT_STOCK" : "STOCK_NOT_FOUND",
              );
            }
          }

          await transaction.inboxEvent.create({
            data: {
              eventId: event.eventId,
              eventType: event.eventType,
              aggregateId: event.data.orderId,
            },
          });
          
          await transaction.outboxEvent.create({
            data: {
              aggregateId: event.data.orderId,
              eventType: STOCK_RESERVATION_CONFIRMED_EVENT,
              payload: {
                orderId: event.data.orderId,
                items,
              },
            },
          });
        },
        { isolationLevel: "Serializable" },
      );
    } catch (error) {
      if (!(error instanceof ReservationRejectedError)) throw error;

      await this.recordRejection(event, error);
    }
  }

  private async recordRejection(
    event: ReservationRequestedEvent,
    error: ReservationRejectedError,
  ) {
    await this.prisma.$transaction(async (transaction) => {
      const alreadyProcessed = await transaction.inboxEvent.findUnique({
        where: { eventId: event.eventId },
      });

      if (alreadyProcessed) return;

      await transaction.inboxEvent.create({
        data: {
          eventId: event.eventId,
          eventType: event.eventType,
          aggregateId: event.data.orderId,
        },
      });

      await transaction.outboxEvent.create({
        data: {
          aggregateId: event.data.orderId,
          eventType: STOCK_RESERVATION_REJECTED_EVENT,
          payload: {
            orderId: event.data.orderId,
            reason: error.reason,
            productId: error.productId,
            requestedQuantity: error.requestedQuantity,
            availableQuantity: error.availableQuantity,
          },
        },
      });
    });
  }

  private aggregateItems(items: ReservationItem[]) {
    const quantities = new Map<string, number>();
    for (const item of items) {
      quantities.set(
        item.productId,
        (quantities.get(item.productId) ?? 0) + item.quantity,
      );
    }
    return [...quantities.entries()]
      .map(([productId, quantity]) => ({ productId, quantity }))
      .sort((a, b) => a.productId.localeCompare(b.productId));
  }

  private parseEvent(message: ConsumeMessage): ReservationRequestedEvent | undefined {
    try {
      const event = JSON.parse(
        message.content.toString(),
      ) as Partial<ReservationRequestedEvent>;
      if (
        event.eventType !== STOCK_RESERVATION_REQUESTED_EVENT ||
        !event.eventId ||
        !UUID_PATTERN.test(event.eventId) ||
        !event.data?.orderId ||
        !UUID_PATTERN.test(event.data.orderId) ||
        !Array.isArray(event.data.items) ||
        event.data.items.length === 0 ||
        !event.data.items.every(
          (item) =>
            !!item &&
            UUID_PATTERN.test(item.productId) &&
            Number.isInteger(item.quantity) &&
            item.quantity > 0,
        )
      ) {
        return undefined;
      }
      return event as ReservationRequestedEvent;
    } catch {
      return undefined;
    }
  }

  private async retryOrDeadLetter(
    channel: ConfirmChannel,
    message: ConsumeMessage,
    error: unknown,
  ) {
    const header = message.properties.headers?.retryCount;
    const retries = typeof header === "number" && Number.isInteger(header) ? header : 0;
    if (retries >= MAX_PROCESSING_RETRIES) {
      this.logger.error(
        { messageId: message.properties.messageId, retries },
        "Solicitação de reserva excedeu as tentativas e foi enviada para a DLQ.",
      );
      channel.nack(message, false, false);
      return;
    }
    try {
      channel.sendToQueue(STOCK_RESERVATION_RETRY_QUEUE, message.content, {
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
        "Falha ao agendar retry da reserva; mensagem enviada para a DLQ.",
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
    this.consumerTag = undefined;
    if (!this.shuttingDown) this.scheduleReconnect();
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
    this.consumerTag = undefined;
    await connection?.close().catch(() => undefined);
  }
}
