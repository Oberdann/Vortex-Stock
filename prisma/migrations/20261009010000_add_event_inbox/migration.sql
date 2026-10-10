CREATE TABLE "InboxEvent" (
    "eventId" UUID NOT NULL,
    "eventType" TEXT NOT NULL,
    "processedAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT "InboxEvent_pkey" PRIMARY KEY ("eventId")
);
