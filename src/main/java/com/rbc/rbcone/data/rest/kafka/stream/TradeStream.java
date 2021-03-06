package com.rbc.rbcone.data.rest.kafka.stream;

import com.google.cloud.firestore.Firestore;
import com.rbc.rbcone.data.rest.kafka.dto.Trade;
import com.rbc.rbcone.data.rest.kafka.dto.firebase.Alert;
import com.rbc.rbcone.data.rest.kafka.util.ElasticSearchService;
import com.rbc.rbcone.data.rest.kafka.util.KafkaProducerInstance;
import com.rbc.rbcone.data.rest.kafka.util.RandomizeTimeStamp;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.stereotype.Component;

import java.io.IOException;

@Component("TradeStream")
public class TradeStream {

    private StreamsBuilder streamsBuilder;

    private Firestore firestore;

    private ElasticSearchService elasticSearchService;

    private KafkaProducerInstance kafkaProducerInstance;

    public TradeStream(StreamsBuilder streamsBuilder, Firestore firestore, ElasticSearchService elasticSearchService, KafkaProducerInstance kafkaProducerInstance) {
        this.streamsBuilder = streamsBuilder;
        this.firestore = firestore;
        this.elasticSearchService = elasticSearchService;
        this.kafkaProducerInstance = kafkaProducerInstance;
        buildFirebaseViewStoreStreams();
    }

    private void buildFirebaseViewStoreStreams() {

        final KStream<String, String> tradeStream = streamsBuilder.stream("replica_trade");
        tradeStream
                .to("kafka_process");
        tradeStream
                .mapValues(Trade::mapTrade)
                .filter(this::filterNonNull)
                .mapValues(this::indexTrade)
                .mapValues(this::sendTradeAlerts);

    }

    private boolean filterNonNull (String key, Trade trade) {
        return trade.getRegion_id() != null;
    }

    private Trade indexTrade(Trade trade) {
        try {
            elasticSearchService.index("replica_trade", trade.getId(), trade.toMap());
        } catch (IOException e) {
            e.printStackTrace();
        }
        return trade;
    }

    private Trade sendTradeAlerts(final Trade trade) {

        /* TODO:
           Need to get average for C and H transactions for the share class / dealer
           in those settlement_amount and quantity if amount or quantity is below 10% avg or above 200% avg raise alert
         */

        if (trade.getStatus_code() != "C" && trade.getStatus_code() != "H") {
            if (trade.getSettlement_amount() != 0 && trade.getSettlement_amount() > 500000) {
                firestore.collection("alerts").add(mapNewTradeOverAmountAlert(trade, 500000));
                kafkaProducerInstance.getProducer().send(new ProducerRecord<String, String>("alert","alert_trade","{}"));
                System.out.println("Sent alert Trade");
            }
            if (trade.getQuantity() != 0 && trade.getQuantity() > 15000) {
                firestore.collection("alerts").add(mapNewTradeOverQuantityAlert(trade, 15000));
                kafkaProducerInstance.getProducer().send(new ProducerRecord<String, String>("alert","alert_trade","{}"));
                System.out.println("Sent alert Trade");
            }

            if (trade.getSettlement_amount() != 0 && trade.getSettlement_amount() < 0.1) {
                firestore.collection("alerts").add(mapNewTradeUnderAmountAlert(trade, 100));
                kafkaProducerInstance.getProducer().send(new ProducerRecord<String, String>("alert","alert_trade","{}"));
                System.out.println("Sent alert Trade");
            }
            if (trade.getQuantity() != 0 && trade.getQuantity() < 0.005) {
                firestore.collection("alerts").add(mapNewTradeUnderQuantityAlert(trade, 1));
                kafkaProducerInstance.getProducer().send(new ProducerRecord<String, String>("alert","alert_trade","{}"));
                System.out.println("Sent alert Trade");
            }
        }
        return trade;
    }

    private Alert mapNewTradeOverAmountAlert(final Trade trade, final long amount) {
        return Alert.builder()
                .id(trade.getId())
                .entity_name(trade.getTrade_id())
                .entity_id(trade.getTrade_id())
                .entity_category("trade")
                .event_category("high_trade_amount")
                .message("Trade for Account " + trade.getAccount_number()
                        + " in share class " + trade.getShare_class_id()
                        + " with id " + trade.getTrade_id()
                        + " for amount " + trade.getSettlement_amount()
                        + " over threshold " + amount
                        + ".")
                .timestamp(RandomizeTimeStamp.getRandom()).build();
    }

    private Alert mapNewTradeOverQuantityAlert(final Trade trade, final long quantity) {
        return Alert.builder()
                .id(trade.getId())
                .entity_name(trade.getTrade_id())
                .entity_id(trade.getTrade_id())
                .entity_category("trade")
                .event_category("high_trade_amount")
                .message("Trade for Account " + trade.getAccount_number()
                        + " in share class " + trade.getShare_class_id()
                        + " with id " + trade.getTrade_id()
                        + " quantity " + trade.getQuantity()
                        + " over threshold " + quantity
                        + ".")
                .timestamp(RandomizeTimeStamp.getRandom()).build();
    }

    private Alert mapNewTradeUnderAmountAlert(final Trade trade, final long amount) {
        return Alert.builder()
                .id(trade.getId())
                .entity_name(trade.getTrade_id())
                .entity_id(trade.getTrade_id())
                .entity_category("trade")
                .event_category("low_trade_amount")
                .message("Trade for Account " + trade.getAccount_number()
                        + " in share class " + trade.getShare_class_id()
                        + " with id " + trade.getTrade_id()
                        + " amount " + trade.getSettlement_amount()
                        + " under threshold " + amount
                        + ".")
                .timestamp(RandomizeTimeStamp.getRandom()).build();
    }

    private Alert mapNewTradeUnderQuantityAlert(final Trade trade, final long quantity) {
        return Alert.builder()
                .id(trade.getId())
                .entity_name(trade.getTrade_id())
                .entity_id(trade.getTrade_id())
                .entity_category("trade")
                .event_category("low_trade_amount")
                .message("Trade for Account " + trade.getAccount_number()
                        + " in share class " + trade.getShare_class_id()
                        + " with id " + trade.getTrade_id()
                        + " quantity " + trade.getQuantity()
                        + " under threshold " + quantity
                        + ".")
                .timestamp(RandomizeTimeStamp.getRandom()).build();
    }

}

