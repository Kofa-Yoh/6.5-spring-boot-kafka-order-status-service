package com.example.orderservice.listeners;

import com.example.orderservice.models.OrderEvent;
import com.example.orderservice.models.OrderStatus;
import net.javacrumbs.jsonunit.JsonAssert;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;

@SpringBootTest
@Testcontainers
class KafkaOrderListenerTest {

    @Container
    static final KafkaContainer kafka = new KafkaContainer(
            DockerImageName.parse("confluentinc/cp-kafka:6.2.0")
    );

    @DynamicPropertySource
    static void registryKafkaProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
    }

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    @Autowired
    private ConsumerFactory<String, Object> consumerFactory;

    @Value("${app.kafka.kafkaOrderTopic}")
    private String orderTopic;

    @Value("${app.kafka.kafkaOrderStatusTopic}")
    private String orderStatusTopic;

    @Test
    public void whenGetOrderEvent_thenSendOrderStatus() {
        String product = "my product";
        Integer quantity = 100;
        OrderEvent orderEvent = new OrderEvent();
        orderEvent.setProduct(product);
        orderEvent.setQuantity(quantity);
        String key = UUID.randomUUID().toString();

        kafkaTemplate.send(orderTopic, key, orderEvent);

        String status = "CREATED";
        Instant date = Instant.now();
        OrderStatus orderStatus = new OrderStatus(status, date);

        await()
                .pollInterval(Duration.ofSeconds(3))
                .atMost(10, TimeUnit.SECONDS)
                .untilAsserted(() -> {
                    Consumer<String, Object> consumer = consumerFactory.createConsumer();
                    List<TopicPartition> partitions = consumer.partitionsFor(orderStatusTopic).stream()
                            .map(p -> new TopicPartition(orderStatusTopic, p.partition()))
                            .toList();
                    consumer.assign(partitions);
                    consumer.seekToBeginning(partitions);

                    ConsumerRecords<String, Object> records = consumer.poll(Duration.ofSeconds(10));

                    for (ConsumerRecord<String, Object> record : records) {
                        JsonAssert.assertJsonEquals(orderStatus, record.value(), JsonAssert.whenIgnoringPaths("date"));
                    }
                });
    }
}