package com.rbc.rbcone.data.rest.kafka.util;

import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;

@Service
public class ElasticSearchService {

    @Autowired
    RestHighLevelClient highElasticClient;

    public boolean isAvailable (String index, String id) throws IOException {
        return highElasticClient.exists(new GetRequest(index,"doc",id));
    }

    public String findOneById (String index, String id) throws IOException {
        return highElasticClient.get(new GetRequest(index,"doc",id)).getSourceAsString();
    }

    public IndexResponse index (String index, String id, String source) throws IOException {
        return highElasticClient.index(new IndexRequest(index, "doc", id).source(source));
    }


}
