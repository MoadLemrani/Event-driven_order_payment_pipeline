ALTER TABLE payment_service.outbox_events ADD COLUMN destination TEXT NOT NULL DEFAULT 'order-events';
ALTER TABLE payment_service.outbox_events ADD COLUMN message_key TEXT;
ALTER TABLE payment_service.outbox_events ALTER COLUMN envelope DROP NOT NULL;
CREATE TABLE payment_service.dead_letters (
    id BIGSERIAL PRIMARY KEY,
    topic TEXT NOT NULL,
    partition_number INTEGER NOT NULL,
    record_offset BIGINT NOT NULL,
    message_key TEXT,
    envelope TEXT,
    error_message TEXT,
    received_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    replay_requested_at TIMESTAMPTZ,
    UNIQUE(topic, partition_number, record_offset)
);
