package com.rbc.rbcone.data.rest.kafka;


import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.firestore.Firestore;
import com.google.firebase.FirebaseApp;
import com.google.firebase.FirebaseOptions;
import com.google.firebase.cloud.FirestoreClient;
import org.apache.http.HttpHost;
import org.apache.kafka.streams.StreamsBuilder;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import java.io.FileInputStream;
import java.io.IOException;

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
        return new RestHighLevelClient(RestClient.builder(
                new HttpHost(serverUrl, port, protocol)));
    }

    @Bean
    public Firestore firestore() throws IOException {
        FileInputStream serviceAccount = new FileInputStream("src/main/resources/Hackathon.json");

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
