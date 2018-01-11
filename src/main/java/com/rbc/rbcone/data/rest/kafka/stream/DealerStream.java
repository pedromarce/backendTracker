package com.rbc.rbcone.data.rest.kafka.stream;

import com.google.cloud.firestore.Firestore;
import com.rbc.rbcone.data.rest.kafka.dto.Account;
import com.rbc.rbcone.data.rest.kafka.dto.Dealer;
import com.rbc.rbcone.data.rest.kafka.dto.firebase.Alert;
import com.rbc.rbcone.data.rest.kafka.util.ElasticSearchService;
import com.rbc.rbcone.data.rest.kafka.util.JacksonMapperDecorator;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.Date;
import java.util.Random;

@Component("DealerStream")
public class DealerStream {

    private StreamsBuilder streamsBuilder;

    private Firestore firestore;

    private ElasticSearchService elasticSearchService;

    public DealerStream(StreamsBuilder streamsBuilder, Firestore firestore, ElasticSearchService elasticSearchService) {
        this.streamsBuilder = streamsBuilder;
        this.firestore = firestore;
        this.elasticSearchService = elasticSearchService;
        buildFirebaseViewStoreStreams();
    }

    private void buildFirebaseViewStoreStreams() {

        final KStream<String, String> dealerStream = streamsBuilder.stream("replica_dealer");
        dealerStream
                .mapValues(Dealer::mapDealer)
                .mapValues(this::indexDealer)
                .mapValues(this::sendDealerAlerts)
                .mapValues(Dealer::mapTrackerIndex)
                .mapValues(JacksonMapperDecorator::writeValueAsString)
                .to("tracker_index");

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
        Random random = new Random();
        if (random.nextInt(3) == 1) /*if sum all holding balance for this dealer id > x then raise alert)*/ {
            firestore.collection("alerts_test").add(mapBalanceDealerAlert(dealer));
            System.out.println("Sent alert dealer");
        }
        return dealer;
    }

    private Alert mapBalanceDealerAlert (final Dealer dealer ) {
        return Alert.builder()
                .id(dealer.getId())
                .entity_name(dealer.getDealer_name())
                .entity_id(dealer.getDealer_id())
                .entity_category("dealer")
                .event_category("dealer_balance")
                .message("Balance for dealer " + dealer.getDealer_id()
                        + ".")
                .timestamp(new Date()).build();
    }
}

