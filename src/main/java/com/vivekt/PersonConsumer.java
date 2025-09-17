package com.vivekt;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.connect.json.JsonDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class PersonConsumer {

    public static void main(String[] args) throws Exception {
        String topic = "persons-topic";
        String groupId = "person-consumer-group";

        // Kafka Consumer Config
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // consume from beginning

        // Create Consumer
        KafkaConsumer<String, JsonNode> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(topic));

        ObjectMapper mapper = new ObjectMapper();

        System.out.println("Listening for Person records...");

        while (true) {
            ConsumerRecords<String, JsonNode> records = consumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<String, JsonNode> record : records) {
                JsonNode jsonNode = record.value();

                // Convert JsonNode back to Person
                Person person = mapper.treeToValue(jsonNode, Person.class);

                System.out.printf("Consumed Person: key=%s, value=%s, partition=%d, offset=%d%n",
                        record.key(), person, record.partition(), record.offset());
            }
        }
    }
}
