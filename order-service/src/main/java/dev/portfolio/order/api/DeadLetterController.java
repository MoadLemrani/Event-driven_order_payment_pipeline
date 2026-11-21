package dev.portfolio.order.api;

import dev.portfolio.order.messaging.DeadLetterStore;
import java.util.List;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.Positive;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/dead-letters")
public class DeadLetterController {
    private final DeadLetterStore store;
    public DeadLetterController(DeadLetterStore store) { this.store = store; }

    @GetMapping
    public List<DeadLetterStore.DeadLetter> list(@RequestParam(defaultValue = "0") @Min(0) long after) {
        return store.list(after);
    }

    @PostMapping("/{id}/replay")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public void replay(@PathVariable @Positive long id) { store.replay(id); }
}
