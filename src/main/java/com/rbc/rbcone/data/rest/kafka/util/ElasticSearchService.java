package com.rbc.rbcone.data.rest.kafka.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rbc.rbcone.data.rest.kafka.dto.elastic.TrackerIndex;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.stats.ParsedStats;
import org.elasticsearch.search.aggregations.metrics.stats.StatsAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Service
public class ElasticSearchService {

    private ObjectMapper mapper = new ObjectMapper();

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

    private SearchResponse search (String index, QueryBuilder query) throws IOException {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(query);
        return highElasticClient.search(new SearchRequest().indices(index).source(searchSourceBuilder));
    }

    private SearchResponse search (String index, QueryBuilder query, AggregationBuilder aggregation) throws IOException {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(query);
        searchSourceBuilder.aggregation(aggregation);
        return highElasticClient.search(new SearchRequest().indices(index).source(searchSourceBuilder));
    }

    public ParsedStats statsIndexByTerm (String index, String field, String value, String accumulator) throws Exception {
        QueryBuilder query = QueryBuilders.termQuery(field + ".keyword",value.toUpperCase());
        StatsAggregationBuilder agg = AggregationBuilders.stats("total").field(accumulator);
        SearchResponse response = this.search(index, field.isEmpty() ? null : query, agg);
        return response.getAggregations().get("total");
    }

    public Map<String, ParsedStats> termsAggregation (String index, String term, String field, String value, String accumulator) throws Exception {
        QueryBuilder query = QueryBuilders.termQuery(field + ".keyword",value.toUpperCase());
        TermsAggregationBuilder agg = AggregationBuilders.terms("by_field").field(term + ".keyword")
                                            .subAggregation(AggregationBuilders.stats("total").field(accumulator));
        SearchResponse response = this.search(index, field.isEmpty() ? null : query, agg);
        Terms terms = response.getAggregations().get("by_field");
        return terms.getBuckets().stream().collect(Collectors.toMap(Terms.Bucket::getKeyAsString,
                (bucket) -> bucket.getAggregations().get("total")));
    }

    public Map<String, ParsedStats> dateHistogramAggregation (String index, String date, String field, String value, String accumulator) throws Exception {
        QueryBuilder query = QueryBuilders.termQuery(field + ".keyword", value.toUpperCase());
        DateHistogramAggregationBuilder agg = AggregationBuilders.dateHistogram("histogram").field(date).dateHistogramInterval(DateHistogramInterval.MONTH)
                .subAggregation(AggregationBuilders.stats("total").field(accumulator));
        SearchResponse response = this.search(index, field.isEmpty() ? null : query, agg);
        Terms terms = response.getAggregations().get("by_field");
        return terms.getBuckets().stream().collect(Collectors.toMap(Terms.Bucket::getKeyAsString,
                (bucket) -> bucket.getAggregations().get("total")));
    }

    public List<TrackerIndex> searchIndex (String index, String value) throws Exception {
        QueryBuilder query = QueryBuilders.multiMatchQuery(value);
        SearchResponse response = this.search(index, query);
        return Arrays.asList(response.getHits().getHits()).stream()
                .map(SearchHit::getSourceAsString)
                .map(this::mapTrackerIndex)
                .collect(Collectors.toList());
    }

    private TrackerIndex mapTrackerIndex (String value) {
        try {
            return mapper.readValue(value, TrackerIndex.class);
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

}
