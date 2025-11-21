package org.hs.mongokafka;

import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;


@SpringBootApplication
public class MongoKafkaApplication {


    public static void main(String[] args) {
        SpringApplication.run(MongoKafkaApplication.class, args);
    }

    @Bean
    public CommandLineRunner run() {
        return args -> {
            MongoListener listener;
            listener.listen();
        };
    }
}
