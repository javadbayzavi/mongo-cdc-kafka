package org.hs.mongokafka.infra;

import org.hs.mongokafka.AppConfig;
import org.hs.mongokafka.MongoChangeProducer;
import org.hs.mongokafka.MongoListener;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class BeansProvider {
    @Bean
    public MongoChangeProducer mongoChangeProducer(AppConfig appConfig) {
        return new MongoChangeProducer(
                appConfig.getKafkaBootstrapServers(),
                appConfig.getKafkaTopic()
        );
    }

    @Bean
    public MongoListener mongoListener(MongoChangeProducer mongoChangeProducer, AppConfig appConfig) {
        return new MongoListener(
                mongoChangeProducer,
                new MongoListener.MongoConfiguration(
                        appConfig.getMongoUri(),
                        appConfig.getMongoDatabase(),
                        appConfig.getMongoCollection()
                )
        );
    }
}
