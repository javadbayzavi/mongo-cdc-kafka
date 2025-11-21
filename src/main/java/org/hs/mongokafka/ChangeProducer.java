package org.hs.mongokafka;

public interface ChangeProducer {
    void sendMessage(String message);
    void close();
}
