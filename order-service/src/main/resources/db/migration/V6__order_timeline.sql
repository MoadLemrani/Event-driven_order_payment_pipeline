CREATE TABLE order_service.order_timeline (
    event_id UUID PRIMARY KEY,
    order_id UUID NOT NULL REFERENCES order_service.orders(id),
    event_type VARCHAR(80) NOT NULL,
    correlation_id VARCHAR(100) NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX order_timeline_order_idx ON order_service.order_timeline(order_id, recorded_at, event_id);
