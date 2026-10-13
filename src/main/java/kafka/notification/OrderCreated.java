package kafka.notification;

import java.math.BigDecimal;
import java.time.Instant;

public record OrderCreated (
        String orderId,
        String customerId,
        String productId,
        int quantity,
        BigDecimal totalAmount,
        Instant createdAt){
}
