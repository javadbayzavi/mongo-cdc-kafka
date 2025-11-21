package org.hs.mongokafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class MongoChangeProducer implements ChangeProducer {
    // Create Kafka producer
    private final Producer<String, String> producer;
    private final String topic;
    public MongoChangeProducer(String kafkaBootstrapServers, String topic) {
        // Kafka producer configuration
        Properties props = new Properties();
        props.put("bootstrap.servers", kafkaBootstrapServers);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<>(props);
        this.topic = topic;
    }

    public void sendMessage(String value){
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, value);
        producer.send(record, (metadata, exception) -> {
            if (exception == null) {
                System.out.printf("Sent change event: %s%n", value);
            } else {
                exception.printStackTrace();
            }
        });
    }

    public void close() {
        producer.close();
    }

}
