package com.vivekt;

//import com.example.kafka.model.UserEvent;
//import org.springframework.beans.factory.annotation.Value;
//import org.springframework.kafka.core.KafkaTemplate;
//import org.springframework.stereotype.Service;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class UserEventProducer {

    private final KafkaTemplate<String, UserEvent> kafkaTemplate;

    @Value("${app.kafka.topic}")
    private String topic;

    public UserEventProducer(KafkaTemplate<String, UserEvent> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendEvent(UserEvent event) {
        kafkaTemplate.send(topic, event);
        System.out.println("Produced event: " + event);
    }
}
