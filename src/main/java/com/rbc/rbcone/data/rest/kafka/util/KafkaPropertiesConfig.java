package com.rbc.rbcone.data.rest.kafka.util;

import lombok.Getter;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.springframework.stereotype.Service;

import java.util.Properties;

@Service
@Getter
public class KafkaPropertiesConfig {

    private Properties config = new Properties();

    public KafkaPropertiesConfig() {
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "index_tracker_hackathon");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "2a481954-kafka0.pub.or.eventador.io:9092");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    }

}
