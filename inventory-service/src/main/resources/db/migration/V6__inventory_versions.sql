-- Existing rows start at version zero; Hibernate checks and increments on each update.
ALTER TABLE inventory_service.stock ADD COLUMN version BIGINT NOT NULL DEFAULT 0;
ALTER TABLE inventory_service.reservations ADD COLUMN version BIGINT NOT NULL DEFAULT 0;
