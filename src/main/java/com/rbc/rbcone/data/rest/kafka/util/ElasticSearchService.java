package com.rbc.rbcone.data.rest.kafka.util;

import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.Map;

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

    public IndexResponse index (String index, String id, Map<String,Object> source) throws IOException {
        return highElasticClient.index(new IndexRequest(index, "doc", id).source(source));
    }

    public SearchResponse stats (String index, QueryBuilder query) throws IOException {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(query);
        searchSourceBuilder.aggregation(AggregationBuilders.stats("stats"));
        return highElasticClient.search(new SearchRequest().source(searchSourceBuilder));
    }

    public SearchResponse terms (String index, QueryBuilder query, String aggregation) throws IOException {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(query);
        searchSourceBuilder.aggregation(AggregationBuilders.terms(aggregation));
        return highElasticClient.search(new SearchRequest().source(searchSourceBuilder));

    }

}
