package com.kafka_stream_demo.listener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.messaging.simp.SimpMessagingTemplate;

@Component
public class KafkaConsumer {

    private Logger log = LoggerFactory.getLogger(KafkaConsumer.class);
    private final SimpMessagingTemplate template;

    public KafkaConsumer(SimpMessagingTemplate template) {
        this.template = template;
    }

    @KafkaListener(topics = "${kafka.output-topic}", groupId = "${spring.kafka.consumer.group-id}")
    public void listenMessages(String data) {

        log.info("Consumed message: " + data);
        template.convertAndSend("/topic/kdemo", data);

    }
}
