package org.example;

import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoCollection;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.bson.Document;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class GetWeatherData {
    private static final String TOPIC_NAME = "weather-topic";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";

    static int count = 0;
    private static double totalTemperature = 0.0;

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "weather-consumer-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        try (Consumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(TOPIC_NAME));

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));

                records.forEach(record -> {
                    String weatherData = record.value();
                    double temperature = extractTemperature(weatherData);

                    saveToBuffer(temperature);
                });

                if(count == 5)
                {
                    double averageTemperature = calculateAverageTemperature();
                    System.out.println("Average Temperature: " + averageTemperature);

                    saveToMongoDB(averageTemperature);
                }

                Thread.sleep(1000);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private static double extractTemperature(String weatherData) {
        return Double.parseDouble(weatherData.split(":")[1].replace("°C", "").trim());
    }

    private static void saveToBuffer(double temperature) {
        totalTemperature += temperature;
        count++;
    }

    private static double calculateAverageTemperature() {
        if (count == 0) {
            return 0.0;
        }
        double averageTemperature = totalTemperature / count;
        totalTemperature = 0.0;
        count = 0;
        return averageTemperature;
    }

    private static void saveToMongoDB(double averageTemperature) {
        try (MongoClient mongoClient = MongoClients.create("mongodb://mymongodb:27017")) {
            MongoDatabase database = mongoClient.getDatabase("weatherDB");
            MongoCollection<Document> collection = database.getCollection("temperatureCollection");

            Document document = new Document("averageTemperature", averageTemperature);
            collection.insertOne(document);

            System.out.println("Average Temperature saved to MongoDataBase: " + averageTemperature);
        }
    }
}

