ALTER TABLE payment_service.payments DROP CONSTRAINT payments_status_check;
ALTER TABLE payment_service.payments ADD CONSTRAINT payments_status_check
    CHECK (status IN ('SUCCEEDED', 'FAILED'));
