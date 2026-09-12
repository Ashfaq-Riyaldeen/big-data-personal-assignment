package com.ashfaq.bigdata.consumer.config;

import com.ashfaq.bigdata.avro.Order;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.DelegatingByTypeSerializer;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * The producer side of the consumer service, used to republish failed records onto the retry
 * topics and the dead letter queue.
 *
 * <p>This exists because a single serialiser is not enough. Two very different kinds of value
 * pass through here:
 *
 * <ul>
 *   <li>A valid {@link Order} that failed processing - serialised as Avro, so the record on the
 *       retry topic or the DLQ is still a readable order that could be replayed.</li>
 *   <li>A <b>poison pill</b>: bytes that were never valid Avro. The error-handling deserialiser
 *       cannot turn these into an {@code Order}, so the original raw bytes are what get
 *       forwarded.</li>
 * </ul>
 *
 * <p>Handing raw bytes to the Avro serialiser fails with {@code Error registering Avro schema
 * "bytes"}. Because the failure happens inside the dead-letter publisher, the record is never
 * recovered, its offset is never committed, and the listener redelivers it forever - a single
 * malformed message silently stops the consumer. Delegating by value type avoids that: each kind
 * of value gets the serialiser it actually needs.
 */
@Configuration
public class KafkaPublishingConfig {

    @Bean
    public ProducerFactory<String, Object> producerFactory(KafkaProperties kafkaProperties) {
        Map<String, Object> config = kafkaProperties.buildProducerProperties(null);

        // Configured from the same properties, so it picks up the Schema Registry URL.
        KafkaAvroSerializer avroSerializer = new KafkaAvroSerializer();
        avroSerializer.configure(config, false);

        Map<Class<?>, Serializer<?>> byType = new LinkedHashMap<>();
        byType.put(Order.class, avroSerializer);
        byType.put(byte[].class, new ByteArraySerializer());

        DefaultKafkaProducerFactory<String, Object> factory =
                new DefaultKafkaProducerFactory<>(config);
        factory.setKeySerializer(new StringSerializer());
        factory.setValueSerializer(new DelegatingByTypeSerializer(byType));
        return factory;
    }

    /**
     * Replaces the auto-configured template, so {@code @RetryableTopic} publishes through the
     * delegating serialiser above.
     */
    @Bean
    public KafkaTemplate<String, Object> kafkaTemplate(ProducerFactory<String, Object> factory) {
        return new KafkaTemplate<>(factory);
    }
}
