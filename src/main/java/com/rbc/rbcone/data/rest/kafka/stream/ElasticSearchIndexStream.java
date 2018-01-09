package com.rbc.rbcone.data.rest.kafka.stream;

import com.fasterxml.jackson.core.type.TypeReference;
import com.rbc.rbcone.data.rest.dto.Dealer;
import com.rbc.rbcone.data.rest.kafka.util.JacksonMapperDecorator;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.springframework.stereotype.Component;

@Component("ElasticSearchIndexStream")
public class ElasticSearchIndexStream {

    private KStreamBuilder kStreamBuilder;

    public ElasticSearchIndexStream(KStreamBuilder kStreamBuilder) {
        this.kStreamBuilder = kStreamBuilder;
        buildDealerViewStoreStreams();
    }

    private void buildDealerViewStoreStreams() {
        final KStream<String, String> DealerStream = kStreamBuilder.stream("replica_dealer");

        DealerStream
                .mapValues(this::mapDealer)
                .mapValues()
                .to("elastic_topic");


    }

    private Dealer mapDealer (final String kafkaMsg) {
        return JacksonMapperDecorator.readValue(kafkaMsg, new TypeReference<Dealer>() {});
    }




    }
}
