package com.rbc.rbcone.data.rest.kafka.stream;

import com.google.cloud.firestore.Firestore;
import com.rbc.rbcone.data.rest.kafka.dto.Dealer;
import com.rbc.rbcone.data.rest.kafka.dto.LegalFund;
import com.rbc.rbcone.data.rest.kafka.dto.firebase.Alert;
import com.rbc.rbcone.data.rest.kafka.util.ElasticSearchService;
import com.rbc.rbcone.data.rest.kafka.util.JacksonMapperDecorator;
import com.rbc.rbcone.data.rest.kafka.util.RandomizeTimeStamp;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.Random;

@Component("LegalFundStream")
public class LegalFundStream {

    private StreamsBuilder streamsBuilder;

    private Firestore firestore;

    private ElasticSearchService elasticSearchService;

    public LegalFundStream(StreamsBuilder streamsBuilder, Firestore firestore, ElasticSearchService elasticSearchService) {
        this.streamsBuilder = streamsBuilder;
        this.firestore = firestore;
        this.elasticSearchService = elasticSearchService;
        buildFirebaseViewStoreStreams();
    }

    private void buildFirebaseViewStoreStreams() {

        final KStream<String, String> legalFundStream = streamsBuilder.stream("replica_legalfund");
        legalFundStream
                .mapValues(LegalFund::mapLegalFund)
                .mapValues(this::indexLegalFund)
                .mapValues(this::sendLegalFundAlerts)
                .mapValues(LegalFund::mapTrackerIndex)
                .mapValues(JacksonMapperDecorator::writeValueAsString)
                .to("tracker_index");

    }

    private LegalFund indexLegalFund(LegalFund legalFund) {
        try {
            elasticSearchService.index("replica_legalfund", legalFund.getId(), legalFund.toMap());
        } catch (IOException e) {
            e.printStackTrace();
        }
        return legalFund;
    }

    private LegalFund sendLegalFundAlerts(final LegalFund legalFund) {
        Random random = new Random();
        if (random.nextInt(10) == 1) {
            firestore.collection("alerts_test").add(mapNewLegalFundAlert(legalFund));
            System.out.println("Sent alert Legal Fund");
        }
        return legalFund;
    }

    private Alert mapNewLegalFundAlert (final LegalFund legalFund) {
        return Alert.builder()
                .id(legalFund.getId())
                .entity_name(legalFund.getLegal_fund_name())
                .entity_id(legalFund.getLegal_fund_id())
                .entity_category("legal_fund")
                .event_category("new_legal_fund")
                .message("New Legal Fund "
                        + " create with code " + legalFund.getLegal_fund_id()
                        + " and name " + legalFund.getLegal_fund_name()
                        + ".")
                .timestamp(RandomizeTimeStamp.getRandom()).build();
    }



}

