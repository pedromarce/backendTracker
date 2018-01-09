package com.rbc.rbcone.data.rest.kafka;


import org.apache.http.HttpHost;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import java.net.InetAddress;

@SpringBootApplication
public class RestToKafkaApplication {

    @Bean(name="KStreamBuilder")
    public KStreamBuilder kStreamBuilder() {
        return new KStreamBuilder();
    }


    @Bean
    public RestHighLevelClient restHighLevelClient(@Value("${elasticsearch.server.url}") String serverUrl,
                                                   @Value("${elasticsearch.port}") Integer port,
                                                   @Value("${elasticsearch.protocol}") String protocol) throws Exception {
        RestClient client = RestClient.builder(
                new HttpHost(InetAddress.getByName(serverUrl), port, protocol)).build();

        return new RestHighLevelClient(client);
    }

    public static void main(final String[] args) {
		SpringApplication.run(RestToKafkaApplication.class, args);
    }

}
