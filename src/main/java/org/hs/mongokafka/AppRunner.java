package org.hs.mongokafka;

import org.springframework.boot.CommandLineRunner;

public class AppRunner implements CommandLineRunner {
    private final MongoListener mongoListener;
    public AppRunner(MongoListener mongoListener) {
        this.mongoListener = mongoListener;
    }

    @Override
    public void run(String... args) throws Exception {
        mongoListener.listen();
    }
}
