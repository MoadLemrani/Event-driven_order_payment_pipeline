package dev.portfolio.payment.application;

import jakarta.validation.constraints.DecimalMax;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.NotNull;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;
import java.math.BigDecimal;

@Validated
@ConfigurationProperties(prefix = "payment")
public record PaymentProperties(
    @NotNull @DecimalMin("0") @DecimalMax("1") BigDecimal failureRate
) {}
