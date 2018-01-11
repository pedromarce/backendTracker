package com.rbc.rbcone.data.rest.kafka.stream;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.cloud.firestore.Firestore;
import com.rbc.rbcone.data.rest.kafka.dto.ShareClass;
import com.rbc.rbcone.data.rest.kafka.dto.firebase.Alert;
import com.rbc.rbcone.data.rest.kafka.util.ElasticSearchService;
import com.rbc.rbcone.data.rest.kafka.util.JacksonMapperDecorator;
import com.rbc.rbcone.data.rest.kafka.util.RandomizeTimeStamp;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.Date;

@Component("ShareClassStream")
public class ShareClassStream {

    private StreamsBuilder streamsBuilder;

    private Firestore firestore;

    private ElasticSearchService elasticSearchService;

    public ShareClassStream(StreamsBuilder streamsBuilder, Firestore firestore, ElasticSearchService elasticSearchService) {
        this.streamsBuilder = streamsBuilder;
        this.firestore = firestore;
        this.elasticSearchService = elasticSearchService;
        buildFirebaseViewStoreStreams();
    }

    private void buildFirebaseViewStoreStreams() {
        final KStream<String, String> shareClassStream = streamsBuilder.stream("replica_shareclass");

        shareClassStream
                .mapValues(ShareClass::mapShareClass)
                .mapValues(this::indexShareClass)
                .mapValues(this::sendShareClassAlerts)
                .mapValues(ShareClass::mapTrackerIndex)
                .mapValues(JacksonMapperDecorator::writeValueAsString)
                .to("tracker_index");

   }

    private ShareClass indexShareClass(ShareClass shareClass) {
        try {
            elasticSearchService.index("replica_shareclass", shareClass.getId(), shareClass.toMap());
        } catch (IOException e) {
            e.printStackTrace();
        }
        return shareClass;
    }

    private ShareClass sendShareClassAlerts(final ShareClass shareClass) {
        try {
            if (!elasticSearchService.isAvailable("replica_shareclass",shareClass.getId())) {
                firestore.collection("alerts_test").add(mapNewShareClassAlert(shareClass));
                System.out.println("Sent alert Share Class");
        if (shareClass.getIs_liquidated()) {
            if (!JacksonMapperDecorator.readValue(elasticSearchService.findOneById("replica_shareclass", shareClass.getId()), new TypeReference<ShareClass>() {}).getIs_liquidated()) {
                firestore.collection("alerts_test").add(mapLiquidatedShareClassAlert(shareClass));
                System.out.println("Sent alert Share Class");
            }
        }
        /*if (sum all holding balance in this share class > 30% sum all share class balance in this legal fund){
            firestore.collection("alerts_test").add(ShareClass.mapBalanceShareClassLegalFundAlert(shareClass));
            System.out.println("Sent alert Share Class");
        }*/
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return shareClass;
    }

    private Alert mapLiquidatedShareClassAlert (final ShareClass shareClass) {
        return Alert.builder()
                .id(shareClass.getId())
                .entity_name(shareClass.getShare_class_name())
                .entity_id(shareClass.getShare_class_id())
                .entity_category("share_class")
                .event_category("liquidated_share_class")
                .message("Share Class " + shareClass.getShare_class_id()
                        + " with code " + shareClass.getShare_class_code()
                        + " and name " + shareClass.getShare_class_name()
                        + " for fund " + shareClass.getLegal_fund_id()
                        + " has been liquidated.")
                .timestamp(RandomizeTimeStamp.getRandom()).build();
    }


    private Alert mapNewShareClassAlert (final ShareClass shareClass) {
        return Alert.builder()
                .id(shareClass.getId())
                .entity_name(shareClass.getShare_class_name())
                .entity_id(shareClass.getShare_class_id())
                .entity_category("share_class")
                .event_category("new_share_class")
                .message("New Share Class " + shareClass.getShare_class_id()
                        + " created with code " + shareClass.getShare_class_code()
                        + " and name " + shareClass.getShare_class_name()
                        + " for fund " + shareClass.getLegal_fund_id()
                        + ".")
                .timestamp(RandomizeTimeStamp.getRandom()).build();
    }

    private Alert mapBalanceShareClassLegalFundAlert (final ShareClass shareClass) {
        return Alert.builder()
                .id(shareClass.getId())
                .entity_name(shareClass.getLegal_fund_id())
                .entity_id(shareClass.getLegal_fund_id())
                .entity_category("legal_fund")
                .event_category("share_class_balance")
                .message("Balance for shareClass " + shareClass.getShare_class_id()
                        + " in legal fund " + shareClass.getLegal_fund_id()
                        + " exceeds 30% of the legal fund"
                        + ".")
                .timestamp(new Date()).build();
    }

}

