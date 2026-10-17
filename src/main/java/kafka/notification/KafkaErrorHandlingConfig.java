package kafka.notification;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.JacksonJsonSerializer;
import org.springframework.kafka.support.serializer.DelegatingByTypeSerializer;
import org.springframework.util.backoff.FixedBackOff;

import java.util.Map;

@Configuration
public class KafkaErrorHandlingConfig {
    @Bean
    public KafkaTemplate<String, Object> dltKafkaTemplate(
            @Value("${spring.kafka.bootstrap-servers}") String bootstrapServers){

        var valueSerializer = new DelegatingByTypeSerializer(Map.of(
                byte[].class, new ByteArraySerializer(),
                OrderCreated.class, new JacksonJsonSerializer<OrderCreated>()));

        var producerFactory = new DefaultKafkaProducerFactory<String, Object>(
                Map.of(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers),
                        new StringSerializer(),
                        valueSerializer);
        return new KafkaTemplate<>(producerFactory);
    }

    @Bean
    public DefaultErrorHandler errorHandler(KafkaTemplate<String, Object> dltKafkaTemplate){
        var recoverer = new DeadLetterPublishingRecoverer(dltKafkaTemplate,
                (record, ex) -> new TopicPartition("orders.DLT", record.partition()));

        var handler = new DefaultErrorHandler(recoverer, new FixedBackOff(1000L, 3));

        handler.addNotRetryableExceptions(IllegalArgumentException.class);

        return handler;
    }

    @Bean
    public NewTopic orderDlt(){
        return TopicBuilder.name("orders.DLT").partitions(3).replicas(1).build();
    }
}
