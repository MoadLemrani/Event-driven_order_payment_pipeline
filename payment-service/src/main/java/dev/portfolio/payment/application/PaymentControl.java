package dev.portfolio.payment.application;

import java.math.BigDecimal;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

@Service
public class PaymentControl {
    private final JdbcTemplate jdbc;
    public PaymentControl(JdbcTemplate jdbc, PaymentProperties initial) {
        this.jdbc = jdbc;
        jdbc.update("INSERT INTO payment_service.payment_control (singleton, failure_rate) VALUES (TRUE, ?) ON CONFLICT DO NOTHING", initial.failureRate());
    }
    public BigDecimal failureRate() {
        return jdbc.queryForObject("SELECT failure_rate FROM payment_service.payment_control WHERE singleton", BigDecimal.class);
    }
    public void setFailureRate(BigDecimal value) {
        jdbc.update("UPDATE payment_service.payment_control SET failure_rate = ? WHERE singleton", value);
    }
}
