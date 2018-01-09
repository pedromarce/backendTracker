package com.rbc.rbcone.data.rest.kafka.stream;

import com.rbc.rbcone.data.rest.kafka.util.KafkaPropertiesConfig;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

@Component
@DependsOn({"TestFirebaseStream"})
public class StreamConfiguration {

    private KafkaPropertiesConfig kafkaPropertiesConfig;

    private StreamsBuilder streamsBuilder;

    public StreamConfiguration(KafkaPropertiesConfig kafkaPropertiesConfig, StreamsBuilder streamsBuilder) {
        this.kafkaPropertiesConfig = kafkaPropertiesConfig;
        this.streamsBuilder = streamsBuilder;
    }

    @PostConstruct
    public void init() {
        Topology topology = streamsBuilder.build();

        KafkaStreams streams = new KafkaStreams(topology, kafkaPropertiesConfig.getConfig());
        streams.start();
    }

}
