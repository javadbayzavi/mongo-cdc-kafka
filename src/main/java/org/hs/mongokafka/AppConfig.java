package org.hs.mongokafka;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

@Configuration
public class AppConfig {

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

    public String getMongoUri() {
        return mongoUri;
    }

    public String getMongoDatabase() {
        return mongoDatabase;
    }

    public String getMongoCollection() {
        return mongoCollection;
    }

    public String getKafkaBootstrapServers() {
        return kafkaBootstrapServers;
    }

    public String getKafkaTopic() {
        return kafkaTopic;
    }
}