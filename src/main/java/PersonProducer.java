package com.example.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.json.JsonSerializer;

import java.util.Properties;

public class PersonProducer {

    public static void main(String[] args) {
        String topic = "persons-topic";

        // Kafka Producer Config
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"); // change if needed
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class.getName());

        // Create Producer
        KafkaProducer<String, Person> producer = new KafkaProducer<>(props);

        try {
            // Create Person instances
            Person p1 = new Person("Alice", 30, "alice@example.com");
            Person p2 = new Person("Bob", 25, "bob@example.com");

            // Send records
            ProducerRecord<String, Person> record1 = new ProducerRecord<>(topic, "person1", p1);
            ProducerRecord<String, Person> record2 = new ProducerRecord<>(topic, "person2", p2);

            producer.send(record1, (metadata, exception) -> {
                if (exception == null) {
                    System.out.printf("Sent: %s to partition %d, offset %d%n",
                            record1.value(), metadata.partition(), metadata.offset());
                } else {
                    exception.printStackTrace();
                }
            });

            producer.send(record2, (metadata, exception) -> {
                if (exception == null) {
                    System.out.printf("Sent: %s to partition %d, offset %d%n",
                            record2.value(), metadata.partition(), metadata.offset());
                } else {
                    exception.printStackTrace();
                }
            });

        } finally {
            producer.flush();
            producer.close();
        }
    }
}
