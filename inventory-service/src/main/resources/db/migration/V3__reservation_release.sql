ALTER TABLE inventory_service.reservations
    ADD COLUMN released BOOLEAN NOT NULL DEFAULT FALSE;
