package com.rbc.rbcone.data.rest.kafka.stream;

import com.google.cloud.firestore.Firestore;
import com.rbc.rbcone.data.rest.kafka.dto.Account;
import com.rbc.rbcone.data.rest.kafka.dto.Dealer;
import com.rbc.rbcone.data.rest.kafka.dto.LegalFund;
import com.rbc.rbcone.data.rest.kafka.dto.ShareClass;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.stereotype.Component;

@Component("TestFirebaseStream")
public class TestFirebaseStream {

    private StreamsBuilder streamsBuilder;

    private Firestore firestore;

    private RestHighLevelClient restHighLevelClient;

    public TestFirebaseStream(StreamsBuilder streamsBuilder, Firestore firestore, RestHighLevelClient restHighLevelClient) {
        this.streamsBuilder = streamsBuilder;
        this.firestore = firestore;
        this.restHighLevelClient = restHighLevelClient;
        buildFirebaseViewStoreStreams();
    }

    private void buildFirebaseViewStoreStreams() {
        final KStream<String, String> shareClassStream = streamsBuilder.stream("replica_shareclass");

        shareClassStream
                .mapValues(ShareClass::mapShareClass)
                .mapValues(this::sendAlert)
                .mapValues(ShareClass::mapTrackerIndex)
                .to("tracker_index");

        final KStream<String, String> dealerStream = streamsBuilder.stream("replica_dealer");
        dealerStream
                .mapValues(Dealer::mapDealer)
                .mapValues(Dealer::mapTrackerIndex)
                .to("tracker_index");

        final KStream<String, String> accountStream = streamsBuilder.stream("replica_account");
        accountStream
                .mapValues(Account::mapAccount)
                .mapValues(Account::mapTrackerIndex)
                .to("tracker_index");

        final KStream<String, String> legalFundStream = streamsBuilder.stream("replica_legalfund");
        legalFundStream
                .mapValues(LegalFund::mapLegalFund)
                .mapValues(LegalFund::mapTrackerIndex)
                .to("tracker_index");

    }

    private ShareClass sendAlert(ShareClass shareClass) {
        firestore.collection("alerts").add(ShareClass.mapNewShareClassAlert(shareClass));
        return shareClass;
    }
}
