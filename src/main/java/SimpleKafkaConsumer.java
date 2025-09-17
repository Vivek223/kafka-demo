import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class SimpleKafkaConsumer {
    public static void main(String[] args) {
        // Kafka configuration settings
        String bootstrapServers = "localhost:9092";  // Replace with your Kafka broker
        String groupId = "my-consumer-group";       // Consumer group ID
        String topic = "my-topic";                  // Replace with your topic name

        // Configure properties
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, 
                  "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, 
                  "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); 
        // earliest = read from beginning, latest = new messages only

        // Create Kafka consumer
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            // Subscribe to the topic
            consumer.subscribe(Collections.singletonList(topic));

            System.out.println("Subscribed to topic: " + topic);

            // Poll loop
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("Consumed message: key=%s, value=%s, partition=%d, offset=%d%n",
                            record.key(), record.value(), record.partition(), record.offset());
                }
            }
        }
    }
}
