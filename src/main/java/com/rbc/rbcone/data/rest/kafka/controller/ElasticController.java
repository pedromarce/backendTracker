package com.rbc.rbcone.data.rest.kafka.controller;

import com.rbc.rbcone.data.rest.kafka.dto.elastic.TrackerIndex;
import com.rbc.rbcone.data.rest.kafka.util.ElasticSearchService;
import org.elasticsearch.search.aggregations.metrics.stats.ParsedStats;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

@CrossOrigin(maxAge = 3600)
@RestController
public class ElasticController {

    @Autowired
    private ElasticSearchService elasticSearchService;

    @RequestMapping("/stats")
    public ParsedStats requestStats (@RequestParam(value="index") String index,
                                     @RequestParam(value="field", defaultValue = "") String field,
                                     @RequestParam(value="value", defaultValue = "") String value,
                                     @RequestParam(value="accumulator") String accumulator) throws Exception{
        return elasticSearchService.statsIndexByTerm(index, field, value, accumulator);
    }

    @RequestMapping("/terms")
    public Map<String, ParsedStats> requestTerms (@RequestParam(value="index") String index,
                                                  @RequestParam(value="term") String term,
                                                  @RequestParam(value="field", defaultValue = "") String field,
                                                  @RequestParam(value="value", defaultValue = "") String value,
                                                  @RequestParam(value="accumulator") String accumulator) throws Exception{
        return elasticSearchService.termsAggregation(index, term, field, value, accumulator);
    }

    @RequestMapping("/date-histogram")
    public Map<String, ParsedStats> requestDateHistogram (@RequestParam(value="index") String index,
                                                  @RequestParam(value="date") String date,
                                                  @RequestParam(value="field") String field,
                                                  @RequestParam(value="value") String value,
                                                  @RequestParam(value="accumulator") String accumulator) throws Exception{
        return elasticSearchService.dateHistogramAggregation(index, date, field, value, accumulator);
    }

    @RequestMapping("/search")
    public List<TrackerIndex> searchEntities (@RequestParam(value="value") String value) throws Exception {
        return elasticSearchService.searchIndex("tracker", value);
    }

}
