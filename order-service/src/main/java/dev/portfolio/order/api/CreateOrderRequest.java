package dev.portfolio.order.api;

import jakarta.validation.constraints.*;
import java.math.BigDecimal;

public record CreateOrderRequest(
    @NotBlank @Pattern(regexp = "[A-Z0-9_-]{1,64}") String sku,
    @Min(1) @Max(1000) int quantity,
    @NotNull @DecimalMin("0.01") @Digits(integer = 12, fraction = 2) BigDecimal totalAmount
) {}
