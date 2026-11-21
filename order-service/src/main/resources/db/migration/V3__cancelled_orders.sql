ALTER TABLE order_service.orders DROP CONSTRAINT orders_status_check;
ALTER TABLE order_service.orders ADD CONSTRAINT orders_status_check
    CHECK (status IN ('CREATED', 'CONFIRMED', 'CANCELLED'));
