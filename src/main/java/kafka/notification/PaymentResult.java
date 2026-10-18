package kafka.notification;

import java.time.Instant;

public record PaymentResult(
        String orderId,
        String status,      // "COMPLETED" or "FAILED"
        String paymentId,   // null when failed
        String reason,      // null when completed
        Instant timestamp) {}
