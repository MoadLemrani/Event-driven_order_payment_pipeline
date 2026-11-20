CREATE TABLE payment_service.payments (
    order_id UUID PRIMARY KEY,
    total_amount NUMERIC(14,2) NOT NULL CHECK (total_amount > 0),
    status VARCHAR(20) NOT NULL CHECK (status = 'SUCCEEDED'),
    processed_at TIMESTAMPTZ NOT NULL
);
