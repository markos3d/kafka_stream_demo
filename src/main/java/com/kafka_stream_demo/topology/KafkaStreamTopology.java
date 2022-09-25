package com.kafka_stream_demo.topology;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

@Configuration
public class KafkaStreamTopology {

        @Value("${kafka.input-topic}")
        private String kafkaInputTopic;

        @Value("${kafka.output-topic}")
        private String kafkaOutputTopic;

        @Autowired
        public void buildPipeline(StreamsBuilder streamsBuilder) {

                final KStream<String, String> stream = streamsBuilder.stream(kafkaInputTopic,
                                Consumed.with(Serdes.String(), Serdes.String()));

                stream.map((key, value) -> KeyValue.pair(key, value.toUpperCase()))
                                .to(kafkaOutputTopic,
                                                Produced.with(Serdes.String(), Serdes.String()));
        }

}
