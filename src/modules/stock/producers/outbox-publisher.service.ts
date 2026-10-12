import { Injectable, OnModuleDestroy, OnModuleInit } from "@nestjs/common";
import * as amqp from "amqplib";
import { ConfirmChannel } from "amqplib";
import { PinoLogger } from "nestjs-pino";
import { PrismaService } from "src/database/prisma.service";
import { RABBITMQ_EVENTS_EXCHANGE } from "src/common/messaging/rabbitmq-topology";

const POLL_INTERVAL_MS = 3000;
const BATCH_SIZE = 25;
const MAX_RETRY_DELAY_MS = 5 * 60 * 1000;

@Injectable()
export class OutboxPublisherService implements OnModuleInit, OnModuleDestroy {
  private pollingTimer?: NodeJS.Timeout;
  private publishing = false;
  private shuttingDown = false;

  constructor(
    private readonly prisma: PrismaService,
    private readonly logger: PinoLogger,
  ) {}

  onModuleInit() {
    if (!process.env.RABBITMQ_URL) {
      this.logger.error("RABBITMQ_URL não foi configurada no Stock Service.");
    }

    this.pollingTimer = setInterval(
      () => void this.publishPendingEvents(),
      POLL_INTERVAL_MS,
    );
    this.pollingTimer.unref();
    void this.publishPendingEvents();
  }

  async onModuleDestroy() {
    this.shuttingDown = true;
    if (this.pollingTimer) clearInterval(this.pollingTimer);
  }

  private async publishPendingEvents() {
    const rabbitMqUrl = process.env.RABBITMQ_URL;
    if (this.publishing || this.shuttingDown || !rabbitMqUrl) return;

    this.publishing = true;
    let connection: amqp.ChannelModel | undefined;
    let channel: ConfirmChannel | undefined;

    try {
      connection = await amqp.connect(rabbitMqUrl);
      channel = await connection.createConfirmChannel();
      await channel.assertExchange(RABBITMQ_EVENTS_EXCHANGE, "topic", {
        durable: true,
      });

      const returnedMessageIds = new Set<string>();
      channel.on("return", (message) => {
        const messageId = message.properties.messageId;
        if (typeof messageId === "string") returnedMessageIds.add(messageId);
      });

      const events = await this.prisma.outboxEvent.findMany({
        where: {
          publishedAt: null,
          nextAttemptAt: { lte: new Date() },
        },
        orderBy: { createdAt: "asc" },
        take: BATCH_SIZE,
      });

      for (const event of events) {
        await this.publishEvent(channel, event, returnedMessageIds);
      }
    } catch (error) {
      this.logger.error(
        { errorType: error instanceof Error ? error.name : "UnknownError" },
        "Falha ao conectar ao RabbitMQ ou consultar a Outbox do Stock.",
      );
    } finally {
      await channel?.close().catch(() => undefined);
      await connection?.close().catch(() => undefined);
      this.publishing = false;
    }
  }

  private async publishEvent(
    channel: ConfirmChannel,
    event: {
      id: string;
      aggregateId: string;
      eventType: string;
      payload: unknown;
      createdAt: Date;
      attempts: number;
    },
    returnedMessageIds: Set<string>,
  ) {
    try {
      const envelope = {
        eventId: event.id,
        eventType: event.eventType,
        occurredAt: event.createdAt.toISOString(),
        data: event.payload,
      };

      channel.publish(
        RABBITMQ_EVENTS_EXCHANGE,
        event.eventType,
        Buffer.from(JSON.stringify(envelope)),
        {
          persistent: true,
          mandatory: true,
          contentType: "application/json",
          messageId: event.id,
          timestamp: event.createdAt.getTime(),
          headers: { aggregateId: event.aggregateId },
        },
      );
      await channel.waitForConfirms();

      if (returnedMessageIds.delete(event.id)) {
        throw new Error("RabbitMQ não encontrou uma fila para o evento.");
      }

      await this.prisma.outboxEvent.update({
        where: { id: event.id },
        data: { publishedAt: new Date(), lastError: null },
      });
    } catch (error) {
      const attempts = event.attempts + 1;
      const delay = Math.min(1000 * 2 ** event.attempts, MAX_RETRY_DELAY_MS);

      await this.prisma.outboxEvent.update({
        where: { id: event.id },
        data: {
          attempts,
          nextAttemptAt: new Date(Date.now() + delay),
          lastError: error instanceof Error ? error.name : "UnknownError",
        },
      });

      this.logger.warn(
        { outboxEventId: event.id, eventType: event.eventType, attempts },
        "Falha ao publicar evento do Stock; nova tentativa agendada.",
      );
    }
  }
}
