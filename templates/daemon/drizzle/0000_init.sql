CREATE TABLE "pings" (
	"id" uuid PRIMARY KEY DEFAULT gen_random_uuid() NOT NULL,
	"note" text NOT NULL,
	"received_at" timestamp with time zone DEFAULT now() NOT NULL
);
