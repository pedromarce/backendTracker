package com.rbc.rbcone.data.rest.kafka.stream;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rbc.rbcone.data.rest.kafka.dto.SaraEvent;
import com.rbc.rbcone.data.rest.kafka.dto.SaraMaster;
import com.rbc.rbcone.data.rest.kafka.dto.ViewStoreEvent;
import com.rbc.rbcone.data.rest.kafka.factory.ViewStoreEventFactory;
import com.rbc.rbcone.data.viewstore.commons.inject.ViewStoreAccount;
import com.rbc.rbcone.data.viewstore.commons.inject.ViewStoreDealer;
import com.rbc.rbcone.data.viewstore.commons.inject.ViewStoreShareClass;
import com.rbc.rbcone.data.viewstore.commons.inject.ViewStoreShareClassCode;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.Date;

@Component("SaraEventMasterStream")
public class SaraEventMasterStream {

    private static final ObjectMapper mapper = new ObjectMapper();

    @Autowired
    private RestHighLevelClient restClient;

    private KStreamBuilder kStreamBuilder;

    public SaraEventMasterStream(KStreamBuilder kStreamBuilder) {
        this.kStreamBuilder = kStreamBuilder;
        buildSaraEventStreams();
    }

    private void buildSaraEventStreams() {
       KStream<String, String> saraEventsMaster = kStreamBuilder.stream("SaraEventsMaster");

       saraEventsMaster
               .filter(this::checkExistingAccount)
               .to("SaraMasterAlertName");
    }

    private boolean checkExistingAccount (final String key, final String value) {
        try {
            final SearchRequest searchRequest = new SearchRequest("accountviewstore");

            SaraMaster saraMaster = mapper.readValue(value, new TypeReference<SaraMaster>(){});
            BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
            boolQueryBuilder.must(new TermQueryBuilder("account_number.keyword", saraMaster.getAccountNumber()))
                            .must(new TermQueryBuilder("region_id.keyword", saraMaster.getRegionId()))
                            .mustNot(new TermQueryBuilder("account_name.keyword", saraMaster.getAccount()[0]));

            searchRequest.source(new SearchSourceBuilder().query(boolQueryBuilder));

            final SearchHits hits = restClient.search(searchRequest).getHits();
            return hits.getHits().length != 0;

        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }


}

