ALTER TABLE "InboxEvent" ADD COLUMN "aggregateId" UUID;

CREATE INDEX "InboxEvent_aggregateId_eventType_idx"
ON "InboxEvent"("aggregateId", "eventType");
