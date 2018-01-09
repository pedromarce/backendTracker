package com.rbc.rbcone.data.rest.kafka.stream;

import com.rbc.rbcone.data.rest.kafka.util.KafkaPropertiesConfig;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

@Component
@DependsOn({"ElasticSearchIndexStream"})
public class StreamConfiguration {

    private KafkaPropertiesConfig kafkaPropertiesConfig;

    private KStreamBuilder kStreamBuilder;

    public StreamConfiguration(KafkaPropertiesConfig kafkaPropertiesConfig, KStreamBuilder kStreamBuilder) {
        this.kafkaPropertiesConfig = kafkaPropertiesConfig;
        this.kStreamBuilder = kStreamBuilder;
    }

    @PostConstruct
    public void init() {
        KafkaStreams streams = new KafkaStreams(kStreamBuilder, kafkaPropertiesConfig.getConfig());
        streams.start();
    }

}
