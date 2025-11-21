package org.hs.mongokafka;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import org.bson.Document;

public class MongoListener {
    private final ChangeProducer producer;
    private final MongoClient mongoClient;
    private MongoDatabase mongoDatabase;
    private MongoCollection<Document> collection;

    public MongoListener(ChangeProducer producer, MongoConfiguration configuration) {
        this.producer = producer;
        this.mongoClient = MongoClients.create(configuration.connectionUri());
        this.mongoDatabase = mongoClient.getDatabase(configuration.database);
        this.collection = mongoDatabase.getCollection(configuration.collection);
    }

    public void listen(){
        try (mongoClient) {
            // Listen to change streams
            collection.watch().forEach((ChangeStreamDocument<Document> change) -> {
                try {
                    // Convert the change event to JSON
                    String changeJson = getJson(change);

                    // Send the change event to Kafka
                    producer.sendMessage(changeJson);
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

    }
    private String getJson(ChangeStreamDocument<Document> change){
        String changeJson;
        String operationType = change.getOperationType().getValue();
        changeJson = switch (operationType) {
            case "insert" ->
                // Handle insert
                    change.getFullDocument().toJson();
            case "update" ->
                // Handle update
                    change.getFullDocument() != null ? change.getFullDocument().toJson() : change.getDocumentKey()
                            .toJson();
            case "delete" ->
                // Handle delete
                    change.getDocumentKey().toJson();
            default ->
                // Handle other operations
                    change.getDocumentKey().toJson();
        };
        return changeJson;
    }

    public record MongoConfiguration(String connectionUri, String database, String collection) {
    }
}
