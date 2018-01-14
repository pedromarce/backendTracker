package com.rbc.rbcone.data.rest.kafka.stream;

import com.google.cloud.firestore.Firestore;
import com.rbc.rbcone.data.rest.kafka.dto.Dealer;
import com.rbc.rbcone.data.rest.kafka.dto.firebase.Alert;
import com.rbc.rbcone.data.rest.kafka.util.ElasticSearchService;
import com.rbc.rbcone.data.rest.kafka.util.JacksonMapperDecorator;
import com.rbc.rbcone.data.rest.kafka.util.KafkaProducerInstance;
import com.rbc.rbcone.data.rest.kafka.util.RandomizeTimeStamp;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.stereotype.Component;

import java.io.IOException;

@Component("DealerStream")
public class DealerStream {

    private StreamsBuilder streamsBuilder;

    private Firestore firestore;

    private ElasticSearchService elasticSearchService;

    private KafkaProducerInstance kafkaProducerInstance;

    public DealerStream(StreamsBuilder streamsBuilder, Firestore firestore, ElasticSearchService elasticSearchService, KafkaProducerInstance kafkaProducerInstance) {
        this.streamsBuilder = streamsBuilder;
        this.firestore = firestore;
        this.elasticSearchService = elasticSearchService;
        this.kafkaProducerInstance = kafkaProducerInstance;
        buildFirebaseViewStoreStreams();
    }

    private void buildFirebaseViewStoreStreams() {

        final KStream<String, String> dealerStream = streamsBuilder.stream("replica_dealer");
        dealerStream
                .to("kafka_process");
        dealerStream
                .mapValues(Dealer::mapDealer)
                .filter(this::filterNonNull)
                .mapValues(this::sendDealerAlerts)
                .mapValues(this::indexDealer)
                .mapValues(Dealer::mapTrackerIndex)
                .mapValues(JacksonMapperDecorator::writeValueAsString)
                .to("tracker_index");

    }

    private boolean filterNonNull (String key, Dealer dealer) {
        return dealer.getRegion_id() != null;
    }

    private Dealer indexDealer(Dealer dealer) {
        try {
            elasticSearchService.index("replica_dealer", dealer.getId(), dealer.toMap());
        } catch (IOException e) {
            e.printStackTrace();
        }
        return dealer;
    }

    private Dealer sendDealerAlerts(final Dealer dealer) {
        try {
            if (!elasticSearchService.isAvailable("replica_dealer",dealer.getId())) {
                Alert alert = mapNewDealerAlert(dealer);
                firestore.collection("alerts").add(alert);
                kafkaProducerInstance.getProducer().send(new ProducerRecord<String, String>("alert","alert_dealer",JacksonMapperDecorator.writeValueAsString(alert)));
                System.out.println("Sent alert Dealer");

            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return dealer;
    }

    private Alert mapNewDealerAlert (final Dealer dealer ) {
        return Alert.builder()
                .id(dealer.getId())
                .entity_name(dealer.getDealer_name())
                .entity_id(dealer.getDealer_id())
                .entity_category("dealer")
                .event_category("new_dealer")
                .message("New dealer created " + dealer.getDealer_id()
                        + " with name " + dealer.getDealer_name()
                        + ".")
                .timestamp(RandomizeTimeStamp.getRandom()).build();
    }
}

