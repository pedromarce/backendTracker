package com.rbc.rbcone.data.rest.kafka.stream;

import com.google.cloud.firestore.Firestore;
import com.rbc.rbcone.data.rest.kafka.dto.LegalFund;
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

@Component("LegalFundStream")
public class LegalFundStream {

    private StreamsBuilder streamsBuilder;

    private Firestore firestore;

    private ElasticSearchService elasticSearchService;

    private KafkaProducerInstance kafkaProducerInstance;

    public LegalFundStream(StreamsBuilder streamsBuilder, Firestore firestore, ElasticSearchService elasticSearchService, KafkaProducerInstance kafkaProducerInstance) {
        this.streamsBuilder = streamsBuilder;
        this.firestore = firestore;
        this.elasticSearchService = elasticSearchService;
        this.kafkaProducerInstance = kafkaProducerInstance;
        buildFirebaseViewStoreStreams();
    }

    private void buildFirebaseViewStoreStreams() {

        final KStream<String, String> legalFundStream = streamsBuilder.stream("replica_legalfund");
        legalFundStream
                .to("kafka_process");
        legalFundStream
                .mapValues(LegalFund::mapLegalFund)
                .filter(this::filterNonNull)
                .mapValues(this::sendLegalFundAlerts)
                .mapValues(this::indexLegalFund)
                .mapValues(LegalFund::mapTrackerIndex)
                .mapValues(JacksonMapperDecorator::writeValueAsString)
                .to("tracker_index");

    }


    private boolean filterNonNull (String key, LegalFund legalFund) {
        return legalFund.getRegion_id() != null;
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
        try {
            if (!elasticSearchService.isAvailable("replica_legalfund",legalFund.getId())) {
                firestore.collection("alerts").add(mapNewLegalFundAlert(legalFund));
                kafkaProducerInstance.getProducer().send(new ProducerRecord<String, String>("alert","alert_legalfund","{}"));
                System.out.println("Sent alert Legal Fund");
            }
        } catch (IOException e) {
            e.printStackTrace();
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

