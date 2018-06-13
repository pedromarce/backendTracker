package com.rbc.rbcone.data.rest.kafka;


import org.apache.http.HttpHost;
import org.apache.kafka.streams.StreamsBuilder;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.Resource;

@SpringBootApplication
public class RestToKafkaApplication {

    @Bean(name="StreamsBuilder")
    public StreamsBuilder streamsBuilder() {
        return new StreamsBuilder();
    }

    @Bean
    public RestHighLevelClient restHighLevelClient(@Value("localhost") String serverUrl,
                                                   @Value("9200") Integer port,
                                                   @Value("http") String protocol) throws Exception {

        RestClientBuilder builder = RestClient.builder(new HttpHost(serverUrl, port, protocol));

        return new RestHighLevelClient(builder);
    }

    @Value("classpath:Hackathon.json")
    private Resource hackathon;

    public static void main(final String[] args) {
		SpringApplication.run(RestToKafkaApplication.class, args);
    }

}
