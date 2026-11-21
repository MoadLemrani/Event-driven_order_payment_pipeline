package dev.portfolio.order.domain;

import org.junit.jupiter.api.Test;
import java.math.BigDecimal;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class OrderTest {
    @Test
    void cancelledOrdersCannotBeConfirmed() {
        var order = new Order("DEMO-SKU", 1, new BigDecimal("9.99"), "trace");
        order.cancel();
        assertThatThrownBy(order::confirm).isInstanceOf(IllegalStateException.class);
        assertThat(order.getStatus()).isEqualTo(OrderStatus.CANCELLED);
    }

    @Test
    void confirmedOrdersCannotBeCancelled() {
        var order = new Order("DEMO-SKU", 1, new BigDecimal("9.99"), "trace");
        order.confirm();
        assertThatThrownBy(order::cancel).isInstanceOf(IllegalStateException.class);
        assertThat(order.getStatus()).isEqualTo(OrderStatus.CONFIRMED);
    }
}
