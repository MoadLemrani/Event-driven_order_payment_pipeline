package dev.portfolio.events;

import java.math.BigDecimal;
import java.util.UUID;

/** A single-SKU order. All amounts are in USD for the portfolio demo. */
public record OrderData(UUID orderId, String sku, int quantity, BigDecimal totalAmount) {}
