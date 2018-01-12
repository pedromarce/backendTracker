package com.rbc.rbcone.data.rest.kafka;

import com.rbc.rbcone.data.rest.kafka.util.ElasticSearchService;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;

@RunWith(SpringRunner.class)
@SpringBootTest
@TestPropertySource(locations="classpath:test.properties")
public class RestToKafkaApplicationTests {


	@Autowired
	ElasticSearchService elasticClient;

	@Test
	public void contextLoads() throws IOException {
		System.out.println(elasticClient.toString());
        elasticClient.isAvailable("replica_dealer","LEMA_RZB");
	//	SearchResponse response = elasticClient.stats("replica_holding", QueryBuilders.termQuery("share_class_id","121"));
		System.out.println(elasticClient.toString());

	}

}
