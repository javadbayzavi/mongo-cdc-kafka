package org.hs.mongokafka;

@FunctionalInterface
public interface ChangeProducer {
    void sendMessage(String message);
}
