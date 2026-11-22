package dev.portfolio.payment.api;

import dev.portfolio.payment.application.PaymentControl;
import jakarta.validation.Valid;
import jakarta.validation.constraints.DecimalMax;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.Digits;
import jakarta.validation.constraints.NotNull;
import java.math.BigDecimal;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/payment-control")
public class PaymentControlController {
    private final PaymentControl control;
    public PaymentControlController(PaymentControl control) { this.control = control; }
    @GetMapping public Settings get() { return new Settings(control.failureRate()); }
    @PutMapping public Settings update(@Valid @RequestBody Settings settings) {
        control.setFailureRate(settings.failureRate());
        return get();
    }
    public record Settings(@NotNull @DecimalMin("0") @DecimalMax("1") @Digits(integer=1, fraction=4) BigDecimal failureRate) {}
}
