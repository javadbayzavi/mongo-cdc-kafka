package org.hs.mongokafka;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.changestream.ChangeStreamDocument;

import org.bson.Document;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import java.util.Properties;

@SpringBootApplication
public class MongoKafkaApplication {
    @Value("${spring.data.mongodb.uri}")
    private String mongoUri;

    @Value("${spring.data.mongodb.database}")
    private String mongoDatabase;

    @Value("${app.mongodb.collection}")
    private String mongoCollection;

    @Value("${spring.kafka.bootstrap-servers}")
    private String kafkaBootstrapServers;

    @Value("${app.kafka.topic}")
    private String kafkaTopic;

    public static void main(String[] args) {
        SpringApplication.run(MongoKafkaApplication.class, args);
    }

    @Bean
    public CommandLineRunner run() {
        return args -> {
            // Kafka producer configuration
            Properties props = new Properties();
            props.put("bootstrap.servers", kafkaBootstrapServers);
            props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

            // Create Kafka producer
            Producer<String, String> producer = new KafkaProducer<>(props);

            // MongoDB client
            try (MongoClient mongoClient = MongoClients.create(mongoUri)) {
                MongoDatabase database = mongoClient.getDatabase(mongoDatabase);
                MongoCollection<Document> collection = database.getCollection(mongoCollection);

                // Listen to change streams
                collection.watch().forEach((ChangeStreamDocument<Document> change) -> {
                    try {
                        // Convert the change event to JSON
                        String changeJson = getJson(change);

                        // Send the change event to Kafka
                        ProducerRecord<String, String> record = new ProducerRecord<>(kafkaTopic, changeJson);
                        producer.send(record, (metadata, exception) -> {
                            if (exception == null) {
                                System.out.printf("Sent change event: %s%n", changeJson);
                            } else {
                                exception.printStackTrace();
                            }
                        });
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                });
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                // Close the Kafka producer
                producer.close();
            }
        };
    }

    private String getJson(ChangeStreamDocument<Document> change){
        String changeJson;
        String operationType = change.getOperationType().getValue();
        switch (operationType) {
            case "insert":
                // Handle insert
                changeJson = change.getFullDocument().toJson();
                break;
            case "update":
                // Handle update
                changeJson = change.getFullDocument() != null ? change.getFullDocument().toJson() : change.getDocumentKey().toJson();
                break;
            case "delete":
                // Handle delete
                changeJson = change.getDocumentKey().toJson();
                break;
            default:
                // Handle other operations
                changeJson = change.getDocumentKey().toJson();
                break;
        }
        return changeJson;
    }
}
