CREATE TABLE inventory_service.stock (
    sku VARCHAR(64) PRIMARY KEY,
    available INTEGER NOT NULL CHECK (available >= 0)
);
CREATE TABLE inventory_service.reservations (
    order_id UUID PRIMARY KEY,
    sku VARCHAR(64) NOT NULL REFERENCES inventory_service.stock(sku),
    quantity INTEGER NOT NULL CHECK (quantity > 0)
);
-- Deliberately small demo catalog; no stock-management API in this project.
INSERT INTO inventory_service.stock (sku, available) VALUES ('DEMO-SKU', 100);
