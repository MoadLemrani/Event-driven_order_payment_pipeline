CREATE TABLE payment_service.payment_control (
    singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
    failure_rate NUMERIC(5, 4) NOT NULL CHECK (failure_rate BETWEEN 0 AND 1)
);
