package com.rbc.rbcone.data.rest.kafka.stream;

import com.google.cloud.firestore.Firestore;
import com.rbc.rbcone.data.rest.kafka.dto.Holding;
import com.rbc.rbcone.data.rest.kafka.dto.firebase.Alert;
import com.rbc.rbcone.data.rest.kafka.util.ElasticSearchService;
import com.rbc.rbcone.data.rest.kafka.util.RandomizeTimeStamp;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.stereotype.Component;

import java.util.Date;
import java.util.Random;

@Component("HoldingStream")
public class HoldingStream {

    private StreamsBuilder streamsBuilder;

    private Firestore firestore;

    private ElasticSearchService elasticSearchService;

    public HoldingStream(StreamsBuilder streamsBuilder, Firestore firestore, ElasticSearchService elasticSearchService) {
        this.streamsBuilder = streamsBuilder;
        this.firestore = firestore;
        this.elasticSearchService = elasticSearchService;
        buildFirebaseViewStoreStreams();
    }

    private void buildFirebaseViewStoreStreams() {

        final KStream<String, String> accountStream = streamsBuilder.stream("replica_holding");
        accountStream
                .mapValues(Holding::mapHolding)
                .mapValues(this::sendHoldingAlerts);

    }

    private Holding sendHoldingAlerts(final Holding holding) {
        Random random = new Random();
        if (holding.getIs_blocked() && random.nextInt(5) == 1) {
            firestore.collection("alerts_test").add(mapBlockHoldingShareClassAlert(holding));
            firestore.collection("alerts_test").add(mapBlockHoldingAccountAlert(holding));
            System.out.println("Sent alert");
        }
        if (holding.getIs_inactive() && random.nextInt(3) == 1) {
            firestore.collection("alerts").add(mapInactiveHoldingShareClassAlert(holding));
            firestore.collection("alerts").add(mapInactiveHoldingAccountAlert(holding));
            System.out.println("Sent alert");
        }
        if (holding.getQuantity() > 100000) {
            firestore.collection("alerts").add(mapBalanceHoldingAccountAlert(holding));
            System.out.println("Sent alert");
        }
        /*if (holding.getQuantity() >  50 % sum all holding balance in this share class){
            firestore.collection("alerts").add(Holding.mapBalanceHoldingClassAlert(holding));
            System.out.println("Sent alert");
        }*/

        return holding;
    }

    private Alert mapBlockHoldingShareClassAlert (final Holding holding) {
        return Alert.builder()
                .id(holding.getId())
                .entity_name(holding.getAccount_number())
                .entity_id(holding.getAccount_number())
                .entity_category("share_class")
                .event_category("holding_blocked")
                .message("Holding for Share Class " + holding.getShare_class_id()
                        + " in account " + holding.getAccount_number()
                        + " has been blocked due to " + holding.getBlocking_reason_code()
                        + ".")
                .timestamp(RandomizeTimeStamp.getRandom()).build();
    }

    private Alert mapBlockHoldingAccountAlert (final Holding holding) {
        return Alert.builder()
                .id(holding.getId())
                .entity_name(holding.getAccount_number())
                .entity_id(holding.getAccount_number())
                .entity_category("account")
                .event_category("holding_blocked")
                .message("Holding for Account " + holding.getAccount_number()
                        + " in share class " + holding.getShare_class_id()
                        + " has been blocked due to " + holding.getBlocking_reason_code()
                        + ".")
                .timestamp(RandomizeTimeStamp.getRandom()).build();
    }

    private Alert mapInactiveHoldingShareClassAlert (final Holding holding) {
        return Alert.builder()
                .id(holding.getId())
                .entity_name(holding.getAccount_number())
                .entity_id(holding.getAccount_number())
                .entity_category("share_class")
                .event_category("holding_inactive")
                .message("Holding for Account " + holding.getAccount_number()
                        + " in share class " + holding.getShare_class_id()
                        + " has been inactive due to " + holding.getBlocking_reason_code()
                        + ".")
                .timestamp(new Date()).build();
    }

    private Alert mapInactiveHoldingAccountAlert (final Holding holding) {
        return Alert.builder()
                .id(holding.getId())
                .entity_name(holding.getAccount_number())
                .entity_id(holding.getAccount_number())
                .entity_category("account")
                .event_category("holding_inactive")
                .message("Holding for Account " + holding.getAccount_number()
                        + " in share class " + holding.getShare_class_id()
                        + " has been inactive due to " + holding.getBlocking_reason_code()
                        + ".")
                .timestamp(new Date()).build();
    }

    private Alert mapBalanceHoldingAccountAlert (final Holding holding) {
        return Alert.builder()
                .id(holding.getId())
                .entity_name(holding.getAccount_number())
                .entity_id(holding.getAccount_number())
                .entity_category("account")
                .event_category("holding_balance")
                .message("Holding for Account " + holding.getAccount_number()
                        + " in share class " + holding.getShare_class_id()
                        + " is " + holding.getQuantity()
                        + ".")
                .timestamp(new Date()).build();
    }

    private Alert mapBalanceHoldingClassAlert (final Holding holding) {
        return Alert.builder()
                .id(holding.getId())
                .entity_name(holding.getAccount_number())
                .entity_id(holding.getAccount_number())
                .entity_category("share_class")
                .event_category("holding_balance")
                .message("Holding for Account " + holding.getAccount_number()
                        + " in share class " + holding.getShare_class_id()
                        + " exceeds 50% of the share class"
                        + ".")
                .timestamp(new Date()).build();
    }

    private Alert mapNewHoldingDealerAlert (final Holding holding) {
        return Alert.builder()
                .id(holding.getId())  /* add dealer id or substring */
                .entity_name(holding.getAccount_number())
                .entity_id(holding.getAccount_number())
                .entity_category("dealer")
                .event_category("new_holding")
                .message("New Holding "
                        + " created with share class " + holding.getShare_class_id()
                        + " and account " + holding.getAccount_number()
                        + " for dealer " + holding.getAccount_number()
                        + ".")
                .timestamp(new Date()).build();
    }

}

