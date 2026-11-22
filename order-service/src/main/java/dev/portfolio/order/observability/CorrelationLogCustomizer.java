package dev.portfolio.order.observability;

import ch.qos.logback.classic.spi.ILoggingEvent;
import org.springframework.boot.json.JsonWriter;
import org.springframework.boot.logging.structured.StructuredLoggingJsonMembersCustomizer;

/** Context-free lifecycle logs explicitly use a sentinel instead of an invented saga trace. */
public class CorrelationLogCustomizer implements StructuredLoggingJsonMembersCustomizer<ILoggingEvent> {
    @Override
    public void customize(JsonWriter.Members<ILoggingEvent> members) {
        members.add("correlationId").when(event -> !event.getMDCPropertyMap().containsKey("correlationId"))
            .as(event -> "-");
    }
}
