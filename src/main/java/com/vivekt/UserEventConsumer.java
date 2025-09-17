package com.vivekt;

//import com.example.kafka.model.UserEvent;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class UserEventConsumer {

    @KafkaListener(topics = "${app.kafka.topic}", groupId = "demo-group")
    public void consume(UserEvent event) {
        System.out.println("Consumed event: " + event);
    }
}
