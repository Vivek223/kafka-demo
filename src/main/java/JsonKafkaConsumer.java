import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class JsonKafkaConsumer {
    public static void main(String[] args) {
        String bootstrapServers = "localhost:9092";  // Replace with your broker
        String groupId = "json-consumer-group";
        String topic = "json-topic";  // Replace with your topic

        // Kafka Consumer configuration
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                  "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                  "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        ObjectMapper mapper = new ObjectMapper();

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(topic));

            System.out.println("Subscribed to topic: " + topic);

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, String> record : records) {
                    try {
                        // Parse JSON string into JsonNode
                        JsonNode jsonNode = mapper.readTree(record.value());

                        // Print parsed JSON
                        System.out.printf("Consumed JSON: %s (partition=%d, offset=%d)%n",
                                jsonNode.toPrettyString(), record.partition(), record.offset());

                        // Example: Access specific fields
                        if (jsonNode.has("id")) {
                            System.out.println("ID field: " + jsonNode.get("id").asInt());
                        }
                        if (jsonNode.has("title")) {
                            System.out.println("Title field: " + jsonNode.get("title").asText());
                        }

                    } catch (Exception e) {
                        System.err.println("Failed to parse JSON message: " + record.value());
                        e.printStackTrace();
                    }
                }
            }
        }
    }
}
