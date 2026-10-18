package kafka.notification;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class PaymentsListener {
    private static final Logger log = LoggerFactory.getLogger(PaymentsListener.class);

    @KafkaListener(topics = "payments", properties = "spring.json.value.default.type=kafka.notification.PaymentResult")
    public void onPayment(PaymentResult result){
        log.info("Powiadomienie: płatność {} dla zamówienia {}", result.status(), result.orderId());
    }
}
