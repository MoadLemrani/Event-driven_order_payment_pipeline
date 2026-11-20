CREATE TABLE order_service.orders (
    id UUID PRIMARY KEY,
    sku VARCHAR(64) NOT NULL,
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    total_amount NUMERIC(14,2) NOT NULL CHECK (total_amount > 0),
    status VARCHAR(20) NOT NULL CHECK (status IN ('CREATED', 'CONFIRMED')),
    correlation_id VARCHAR(100) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL
);
