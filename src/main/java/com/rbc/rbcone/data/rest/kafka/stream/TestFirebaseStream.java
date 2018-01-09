package com.rbc.rbcone.data.rest.kafka.stream;

import com.google.cloud.firestore.Firestore;
import com.rbc.rbcone.data.rest.kafka.dto.ShareClass;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.stereotype.Component;

import java.util.Date;
import java.util.HashMap;

@Component("TestFirebaseStream")
public class TestFirebaseStream {

    private StreamsBuilder streamsBuilder;

    private Firestore firestore;

    private RestHighLevelClient restHighLevelClient;

    public TestFirebaseStream(StreamsBuilder streamsBuilder, Firestore firestore, RestHighLevelClient restHighLevelClient) {
        this.streamsBuilder = streamsBuilder;
        this.firestore = firestore;
        this.restHighLevelClient = restHighLevelClient;
        buildAccountViewStoreStreams();
    }

    private void buildAccountViewStoreStreams() {
        final KStream<String, String> accountStream = streamsBuilder.stream("replica_shareclass");

        accountStream
                .mapValues(ShareClass::mapShareClass)
                .mapValues(this::sendAlert)
                .mapValues(ShareClass::mapTrackerIndex)
                .to("tracker_index");
    }

    private ShareClass sendAlert(ShareClass shareClass) {
        firestore.collection("alerts").add(ShareClass.mapNewShareClassAlert(shareClass));
        return shareClass;
    }
}
