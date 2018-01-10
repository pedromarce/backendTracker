package com.rbc.rbcone.data.rest.kafka.stream;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.cloud.firestore.Firestore;
import com.rbc.rbcone.data.rest.kafka.dto.ShareClass;
import com.rbc.rbcone.data.rest.kafka.dto.TrackerIndex;
import com.rbc.rbcone.data.rest.kafka.util.JacksonMapperDecorator;
import org.apache.commons.codec.binary.Base64;
import org.apache.http.Header;
import org.apache.http.message.BasicHeader;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.stereotype.Component;

import java.io.IOException;

@Component("ElasticSearchIndex")
public class ElasticSearchIndex {

   /* private StreamsBuilder streamsBuilder;

    private Firestore firestore;

    private RestHighLevelClient restHighLevelClient;

    public ElasticSearchIndex(StreamsBuilder streamsBuilder, Firestore firestore, RestHighLevelClient restHighLevelClient) {
        this.streamsBuilder = streamsBuilder;
        this.firestore = firestore;
        this.restHighLevelClient = restHighLevelClient;
        buildAccountViewStoreStreams();
    }

    private void buildAccountViewStoreStreams() {
        final KStream<String, String> accountStream = streamsBuilder.stream("tracker_index");

        accountStream
                .mapValues(this::indexElastic);
    }



    private TrackerIndex indexElastic (final String jsonObject) {
        try {


            byte[] authEncBytes = Base64.encodeBase64("elastic:oHUd3Y9h7UXvWVLhW6bKRdnu@".getBytes());
            String authStringEnc = new String(authEncBytes);
            TrackerIndex idx = JacksonMapperDecorator.readValue(jsonObject, new TypeReference<TrackerIndex>() {});

            IndexRequest indexRequest = new IndexRequest("tracker","tracker",idx.getEntity_category() + "_" + idx.getEntity()).source(JacksonMapperDecorator.writeAsMap(idx));
            Header[] headers = {
                    new BasicHeader("Authorization", "Basic " + authStringEnc),
            };




            restHighLevelClient.index(indexRequest, headers );
            return idx;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    } */
}
