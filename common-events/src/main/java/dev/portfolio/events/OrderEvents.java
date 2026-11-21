package dev.portfolio.events;

public final class OrderEvents {
    public static final String TOPIC = "order-events";
    public static final String CREATED = "OrderCreated";
    public static final String INVENTORY_RESERVED = "InventoryReserved";
    public static final String PAYMENT_SUCCEEDED = "PaymentSucceeded";
    public static final String CONFIRMED = "OrderConfirmed";

    public static final String INVENTORY_FAILED = "InventoryFailed";
    public static final String PAYMENT_FAILED = "PaymentFailed";
    public static final String INVENTORY_RELEASED = "InventoryReleased";
    public static final String CANCELLED = "OrderCancelled";

    private OrderEvents() {}
}
