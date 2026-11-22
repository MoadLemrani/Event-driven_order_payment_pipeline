package dev.portfolio.order.observability;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.UUID;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.http.ProblemDetail;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
public class CorrelationFilter extends OncePerRequestFilter {
    private final ObjectMapper json;
    public CorrelationFilter(ObjectMapper json) { this.json = json; }

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain chain)
            throws ServletException, IOException {
        String supplied = request.getHeader("X-Correlation-ID");
        boolean valid = supplied == null || supplied.matches("[A-Za-z0-9._-]{1,100}");
        String trace = valid && supplied != null ? supplied : UUID.randomUUID().toString();
        var previous = MDC.getCopyOfContextMap();
        MDC.put("correlationId", trace);
        response.setHeader("X-Correlation-ID", trace);
        try {
            if (!valid) {
                response.setStatus(400);
                response.setContentType("application/problem+json");
                json.writeValue(response.getOutputStream(), ProblemDetail.forStatusAndDetail(
                    HttpStatus.BAD_REQUEST, "X-Correlation-ID must contain 1–100 letters, digits, dots, underscores or hyphens"));
                return;
            }
            chain.doFilter(request, response);
        } finally {
            // A successful order lookup returns the persisted saga trace.
            MDC.put("correlationId", response.getHeader("X-Correlation-ID"));
            if (!request.getRequestURI().startsWith("/actuator/")) {
                LoggerFactory.getLogger(CorrelationFilter.class).info("HTTP {} {} status={}",
                    request.getMethod(), request.getRequestURI(), response.getStatus());
            }
            if (previous == null) MDC.clear(); else MDC.setContextMap(previous);
        }
    }
}
