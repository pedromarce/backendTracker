package com.rbc.rbcone.data.rest.kafka.stream;

import com.google.cloud.firestore.Firestore;
import com.rbc.rbcone.data.rest.kafka.dto.Account;
import com.rbc.rbcone.data.rest.kafka.dto.ShareClass;
import com.rbc.rbcone.data.rest.kafka.util.ElasticSearchService;
import com.rbc.rbcone.data.rest.kafka.util.JacksonMapperDecorator;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.stereotype.Component;

import java.io.IOException;

@Component("AccountStream")
public class AccountStream {

    private StreamsBuilder streamsBuilder;

    private Firestore firestore;

    private ElasticSearchService elasticSearchService;

    public AccountStream(StreamsBuilder streamsBuilder, Firestore firestore, ElasticSearchService elasticSearchService) {
        this.streamsBuilder = streamsBuilder;
        this.firestore = firestore;
        this.elasticSearchService = elasticSearchService;
        buildFirebaseViewStoreStreams();
    }

    private void buildFirebaseViewStoreStreams() {

        final KStream<String, String> accountStream = streamsBuilder.stream("replica_account");
        accountStream
                .to("kafka_process");

        accountStream
                .mapValues(Account::mapAccount)
                .filter(this::filterNonNull)
                .mapValues(this::indexAccount)
                .mapValues(Account::mapTrackerIndex)
                .mapValues(JacksonMapperDecorator::writeValueAsString)
                .to("tracker_index");

    }

    private boolean filterNonNull (String key, Account account) {
        return account.getRegion_id() != null;
    }

    private Account indexAccount(Account account) {
        try {
            elasticSearchService.index("replica_account", account.getId(), account.toMap());
        } catch (IOException e) {
            e.printStackTrace();
        }
        return account;
    }

}

