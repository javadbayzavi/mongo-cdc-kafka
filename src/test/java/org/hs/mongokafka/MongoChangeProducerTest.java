package org.hs.mongokafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.kafka.KafkaContainer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Properties;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

class MongoChangeProducerTest {
    private  MongoChangeProducer producer;
    private  KafkaConsumer<String, String> consumer;

    private  KafkaContainer kafkaContainer;
    @BeforeEach
    void beforeEach() {
        kafkaContainer = new KafkaContainer("apache/kafka-native:3.8.0");
        kafkaContainer.start();
        producer = new MongoChangeProducer(
                kafkaContainer.getBootstrapServers(),
                "test"
        );

        consumer = getConsumer();
        consumer.subscribe(Collections.singletonList("test"));
    }

    @Test
    void shouldProduceChangeMessage() {
        //Given
        String message = "test";

        //When
        producer.sendMessage(message);

        //Then
        ConsumerRecords<String, String> records = consumer.poll(Duration.of(2 , ChronoUnit.SECONDS));
        assertThat(records.count(), is(1));
        assertThat(records.records("test").iterator().next().value(), is(message));

    }

    private KafkaConsumer<String, String> getConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        return new KafkaConsumer<>(props);
    }

    @AfterEach
    void afterEach() {
        producer.close();
        kafkaContainer.stop();
    }

}