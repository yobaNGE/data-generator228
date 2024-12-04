package org.example;

import com.github.javafaker.Faker;
import com.mongodb.client.*;
import com.mongodb.client.model.Indexes;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriterSettings;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.TimeUnit;

public class MainTest {
    // Поля для доступа к базе данных
    private MongoClient mongoClient;
    private MongoDatabase database;
    private MongoCollection<Document> collection;

    // Инициализация подключения к базе данных
    @Before
    public void setupConnection() {
        mongoClient = MongoClients.create(URI);
        database = mongoClient.getDatabase(DATABASE_NAME);
        collection = database.getCollection(COLLECTION_NAME);
        System.out.println("Setup is done!\nBegin testing...");
    }

    // Закрытие подключения к базе данных после тестов
    @After
    public void closeConnection() {
        mongoClient.close();
    }

    private final String URI = "mongodb://localhost:27017";
    private final String DATABASE_NAME = "lab2db";
    private final String COLLECTION_NAME = "review";
    private final List<String> REVIEWER_LIST = List.of("Лешь15", "Анна89", "Игорь_Индезит", "Витек_Самсунг", "Наталья_Ксяоми");
    private final List<String> TAGS_LIST = List.of("полезное", "уморительно", "спам", "уныние", "продуктивно", "скука");
    private final Faker faker = new Faker();
    private final JsonWriterSettings jsonSettings = JsonWriterSettings.builder()
            .indent(true)
            .outputMode(JsonMode.SHELL)
            .build();

    public <T> T oneOf(List<T> list) {
        return list.get(faker.random().nextInt(list.size()));
    }

    Set<String> tags = new HashSet<>();

    public <T> Set<T> fewOf(List<T> list, int uniqueCount) {
        Set<T> uniqueValues = new HashSet<>();
        while (uniqueValues.size() < uniqueCount) {
            uniqueValues.add(list.get(faker.random().nextInt(list.size())));
        }
        return uniqueValues;
    }
    @Test
    public void createIndexes() {
        collection.createIndex(Indexes.ascending("tags"));
        System.out.println("Indexes created successfully!");
    }
    // Метод по генерации данных
    public Document generateData() {
        Map<String, Object> doc = new LinkedHashMap<>();
        doc.put("username", oneOf(REVIEWER_LIST));
        doc.put("product", faker.commerce().productName());
        doc.put("rating", faker.number().numberBetween(1, 6));
        doc.put("comment", faker.lorem().paragraph(faker.random().nextInt(1, 5)));
        doc.put("date", faker.date().past(365, TimeUnit.DAYS));
        doc.put("tags", fewOf(TAGS_LIST, faker.random().nextInt(1, 3)));

        return new Document(doc);
    }

    // Метод по выводу 5 сгенерированных значений в консоль
    @Test
    public void writeToConsole() {
        for (int i = 0; i < 5; i++) {
            System.out.println(generateData().toJson(jsonSettings));
        }
    }

    // Метод по записи 10000 сгенирированных значений в базу данных
    @Test
    public void writeToDatabase() {
        List<Document> documents = new ArrayList<>();
        for (int i = 0; i < 1_000_00; i++) {
            documents.add(generateData());
        }
        collection.insertMany(documents);
    }

    @Test
    public void explainRequest() {
        Document result = collection
                .find(BsonDocument.parse("{username: 'Анна89'}"))
                .explain();
        System.out.println(result.toJson(jsonSettings));
    }

    @Test
    public void request() {
        FindIterable<Document> results = collection
                .find(BsonDocument.parse("{$expr : { $gt : [ { $strLenCP : ' $owner ' }, 4 ] } }"))
                .limit(10);
        for (Document result : results) {
            System.out.println(result.toJson(jsonSettings));
        }
    }

    @Test
    public void aggregationRequest() {
        List<Bson> pipeline = new ArrayList<>();
        pipeline.add(
                BsonDocument.parse("""
            {
                $bucket: {
                    groupBy: '$username',
                    boundaries: ['Анна89', 'Лешь15'],
                    default: 'Others',
                    output: {
                        count: { $sum: 1 }
                    }
                }
            }
            """)
        );
        AggregateIterable<Document> results = collection.aggregate(pipeline);
        for (Document result : results) {
            System.out.println(result.toJson(jsonSettings));
        }
    }
}