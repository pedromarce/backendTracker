package com.rbc.rbcone.data.rest.kafka;

import com.rbc.rbcone.data.rest.kafka.util.ElasticSearchService;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest
@TestPropertySource(locations="classpath:test.properties")
public class RestToKafkaApplicationTests {


	@Autowired
	ElasticSearchService elasticClient;

	@Test
	public void contextLoads() throws Exception {
		System.out.println(elasticClient.toString());
        elasticClient.isAvailable("replica_dealer","LEMA_RZB");
		elasticClient.statsIndexByTerm("replica_trade","dealer_id", "LEMA_DFA", "quantity");
		elasticClient.termsAggregation("replica_trade", "account_number","dealer_id", "LEMA_DFA", "quantity");
		elasticClient.searchIndex("tracker","AELIN");
		System.out.println(elasticClient.toString());

	}

}
