package com.rbc.rbcone.data.rest.kafka;


import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.firestore.Firestore;
import com.google.firebase.FirebaseApp;
import com.google.firebase.FirebaseOptions;
import com.google.firebase.cloud.FirestoreClient;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.streams.StreamsBuilder;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.Resource;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

@SpringBootApplication
public class RestToKafkaApplication {

    @Bean(name="StreamsBuilder")
    public StreamsBuilder streamsBuilder() {
        return new StreamsBuilder();
    }

    @Bean
    public RestHighLevelClient restHighLevelClient(@Value("${elasticsearch.server.url}") String serverUrl,
                                                   @Value("${elasticsearch.port}") Integer port,
                                                   @Value("${elasticsearch.protocol}") String protocol) throws Exception {

        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials("elastic", "oHUd3Y9h7UXvWVLhW6bKRdnu"));

        RestClientBuilder builder = RestClient.builder(new HttpHost(serverUrl, port, protocol))
                .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                    @Override
                    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
                        return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                    }
                });

        return new RestHighLevelClient(builder);
    }

    @Value("classpath:Hackathon.json")
    private Resource hackathon;

    @Bean
    public Firestore firestore() throws IOException {
        InputStream serviceAccount = hackathon.getInputStream();

        FirebaseOptions options = new FirebaseOptions.Builder()
                .setCredentials(GoogleCredentials.fromStream(serviceAccount))
                .setDatabaseUrl("https://hackathon-60430.firebaseio.com")
                .build();

        return FirestoreClient.getFirestore(FirebaseApp.initializeApp(options));
    }


    public static void main(final String[] args) {
		SpringApplication.run(RestToKafkaApplication.class, args);
    }

}
