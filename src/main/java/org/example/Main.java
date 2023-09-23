package org.example;

import com.github.javafaker.Faker;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.util.*;
import java.util.concurrent.TimeUnit;

public class Main {
    private static final String URI = "mongodb://localhost:27017";
    private static final String DATABASE_NAME = "mydatabase";
    private static final String COLLECTION_NAME = "books";
    private static final List<String> OWNER_LIST = Arrays.asList("Bob", "Mike", "Jake");

    private static final Faker faker = new Faker();

    public static Document generateData() {
        Map<String, Object> doc = new LinkedHashMap<>();
        doc.put("title", faker.book().title());
        doc.put("author", faker.book().author());
        doc.put("year", faker.date().past(1, TimeUnit.DAYS));
        doc.put("inStock", faker.bool().bool());
        doc.put("arr", Arrays.asList(1, 2, 3));
        doc.put("owner", oneOf(OWNER_LIST));
        return new Document(doc);
    }

    public static <T> T oneOf(List<T> list) {
        return list.get(faker.random().nextInt(list.size()));
    }

    public static void main(String[] args) {
        try (MongoClient mongoClient = MongoClients.create(URI)) {
            MongoDatabase database = mongoClient.getDatabase(DATABASE_NAME);
            MongoCollection<Document> collection = database.getCollection(COLLECTION_NAME);
            for (int i=0; i<10_000; i++) {
                collection.insertOne(generateData());
                if ((i+1)%10 == 0) {
                    System.out.println("In process: " + (i+1));
                }
            }
            System.out.println("Done!");
        }
    }
}