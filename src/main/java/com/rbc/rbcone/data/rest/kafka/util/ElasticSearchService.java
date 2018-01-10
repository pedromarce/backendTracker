package com.rbc.rbcone.data.rest.kafka.util;

import com.fasterxml.jackson.core.type.TypeReference;
import com.rbc.rbcone.data.rest.kafka.dto.elastic.Hit;
import org.apache.http.Header;
import org.apache.http.message.BasicHeader;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@Service
public class ElasticSearchService {

    @Autowired
    RestClient elasticClient;

    private Map<String, String> params;
    private Header[] headers;


    ElasticSearchService () {
        params = new HashMap<>();
        params.put("pretty", "true");
        headers = new Header[]{new BasicHeader("Authorization", "Basic ZWxhc3RpYzpvSFVkM1k5aDdVWHZXVkxoVzZiS1JkbnU=")};
    }

    public boolean isAvailable (String index, String id) throws IOException {


        Response response = elasticClient.performRequest("GET", "/" + index + "/doc/" + id, params, headers);
        Hit hit = JacksonMapperDecorator.readValue(EntityUtils.toString(response.getEntity()), new TypeReference<Hit>() {
        });
        return hit.isFound();
    }


}
