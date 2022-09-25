package com.kafka_stream_demo.web.rest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api")
public class KafkaResource {

    @Value("${kafka.input-topic}")
    private String kafkaInputTopic;

    private final Logger log = LoggerFactory.getLogger(KafkaResource.class);
    private KafkaTemplate<String, String> kafkaTemplate;

    public KafkaResource(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @PostMapping("/send")
    public ResponseEntity<Void> sendNotification(@RequestBody String message) {
        log.info("Post Request with @RequestBody " + message);
        kafkaTemplate.send(kafkaInputTopic, message);

        return new ResponseEntity<>(HttpStatus.OK);
    }

}