CREATE TABLE payment_service.outbox_events (
    id BIGSERIAL PRIMARY KEY,
    event_id UUID NOT NULL UNIQUE,
    aggregate_id UUID NOT NULL,
    event_type VARCHAR(80) NOT NULL,
    correlation_id VARCHAR(100) NOT NULL,
    envelope TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    sent_at TIMESTAMPTZ
);
CREATE INDEX outbox_pending_idx ON payment_service.outbox_events (id) WHERE sent_at IS NULL;
CREATE TABLE payment_service.processed_events (
    event_id UUID PRIMARY KEY,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);
