package org.example;

import com.github.javafaker.Faker;
import com.mongodb.client.*;
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

import static org.junit.Assert.*;

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
        System.out.println("Done!");
    }

    // Закрытие подключения к базе данных после тестов
    @After
    public void closeConnection() {
        mongoClient.close();
    }

    private final String URI = "mongodb://localhost:27017";
    private final String DATABASE_NAME = "mydatabase";
    private final String COLLECTION_NAME = "books";
    private final List<String> OWNER_LIST = Arrays.asList("Bob", "Mike", "Jake");
    private final Faker faker = new Faker();
    private final JsonWriterSettings jsonSettings = JsonWriterSettings.builder()
            .indent(true)
            .outputMode(JsonMode.SHELL)
            .build();
    public <T> T oneOf(List<T> list) {
        return list.get(faker.random().nextInt(list.size()));
    }

    // Метод по генерации данных
    public Document generateData() {
        Map<String, Object> doc = new LinkedHashMap<>();
        doc.put("title", faker.book().title());
        doc.put("author", faker.book().author());
        doc.put("year", faker.date().past(1, TimeUnit.DAYS));
        doc.put("inStock", faker.bool().bool());
        doc.put("arr", Arrays.asList(1, 2, 3));
        doc.put("owner", oneOf(OWNER_LIST));
        return new Document(doc);
    }

    // Метод по выводу 5 сгенерированных значений в консоль
    @Test
    public void writeToConsole() {
        for (int i=0; i<5; i++) {
            System.out.println(generateData().toJson(jsonSettings));
        }
    }

    // Метод по записи 10000 сгенирированных значений в базу данных
    @Test
    public void writeToDatabase() {
        List<Document> documents = new ArrayList<>();
        for (int i=0; i<10000; i++) {
            documents.add(generateData());
        }
        collection.insertMany(documents);
    }

    @Test
    public void explainRequest() {
        Document result = collection.find(BsonDocument.parse("{owner: 'Mike'}")).explain();
        System.out.println(result.toJson(jsonSettings));
    }

    @Test
    public void request() {
        FindIterable<Document> results = collection.find(BsonDocument.parse("{$expr : { $gt : [ { $strLenCP : ' $owner ' }, 4 ] } }")).limit(10);
        for (Document result : results) {
            System.out.println(result.toJson(jsonSettings));
        }
    }

    @Test
    public void aggregationRequest() {
        List<Bson> pipeline = new ArrayList<>();
        pipeline.add(
                BsonDocument.parse("{" +
                        "    $bucket:" +
                        "      {" +
                        "        groupBy: '$owner'," +
                        "        boundaries: ['Bob', 'Jake', 'Mike']," +
                        "        default: 'Others'," +
                        "        output: {" +
                        "          count: {" +
                        "            $sum: 1," +
                        "          }," +
                        "        }," +
                        "      }," +
                        "  },")
        );
        AggregateIterable<Document> results = collection.aggregate(pipeline);
        for (Document result : results) {
            System.out.println(result.toJson(jsonSettings));
        }
    }
}