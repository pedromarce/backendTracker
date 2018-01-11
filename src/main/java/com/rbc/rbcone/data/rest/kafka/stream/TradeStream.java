package com.rbc.rbcone.data.rest.kafka.stream;

import com.google.cloud.firestore.Firestore;
import com.rbc.rbcone.data.rest.kafka.dto.Trade;
import com.rbc.rbcone.data.rest.kafka.dto.firebase.Alert;
import com.rbc.rbcone.data.rest.kafka.util.ElasticSearchService;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.Date;

@Component("TradeStream")
public class TradeStream {

    private StreamsBuilder streamsBuilder;

    private Firestore firestore;

    private ElasticSearchService elasticSearchService;

    public TradeStream(StreamsBuilder streamsBuilder, Firestore firestore, ElasticSearchService elasticSearchService) {
        this.streamsBuilder = streamsBuilder;
        this.firestore = firestore;
        this.elasticSearchService = elasticSearchService;
        buildFirebaseViewStoreStreams();
    }

    private void buildFirebaseViewStoreStreams() {

        final KStream<String, String> accountStream = streamsBuilder.stream("replica_trade");
        accountStream
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
            elasticSearchService.index("replica_legalfund", trade.getId(), trade.toMap());
        } catch (IOException e) {
            e.printStackTrace();
        }
        return trade;
    }

    private Trade sendTradeAlerts(final Trade trade) {

        if (trade.getSettlement_amount() != 0 && trade.getSettlement_amount() > 250000) {
            firestore.collection("alerts").add(mapNewTradeOverAmountAlert(trade, 250000));
            System.out.println("Sent alert Trade");
        }
        if (trade.getQuantity() != 0 && trade.getQuantity() > 10000) {
            firestore.collection("alerts").add(mapNewTradeOverQuantityAlert(trade, 10000));
            System.out.println("Sent alert Trade");
        }

        if (trade.getSettlement_amount() != 0 && trade.getSettlement_amount() < 5000) {
            firestore.collection("alerts").add(mapNewTradeUnderAmountAlert(trade, 5000));
            System.out.println("Sent alert Trade");
        }
        if (trade.getQuantity() != 0 && trade.getQuantity() < 50) {
            firestore.collection("alerts").add(mapNewTradeUnderQuantityAlert(trade, 50));
            System.out.println("Sent alert Trade");
        }
        return trade;
    }

    private Alert mapNewTradeOverAmountAlert(final Trade trade, final long amount) {
        return Alert.builder()
                .id(trade.getId())
                .entity_name(trade.getTrade_id())
                .entity_id(trade.getTrade_id())
                .entity_category("trade")
                .event_category("trade_amount")
                .message("Trade for Account " + trade.getAccount_number()
                        + " in share class " + trade.getShare_class_id()
                        + " with id " + trade.getTrade_id()
                        + " for amount " + trade.getSettlement_amount()
                        + " over threshold " + amount
                        + ".")
                .timestamp(new Date()).build();
    }

    private Alert mapNewTradeOverQuantityAlert(final Trade trade, final long quantity) {
        return Alert.builder()
                .id(trade.getId())
                .entity_name(trade.getTrade_id())
                .entity_id(trade.getTrade_id())
                .entity_category("trade")
                .event_category("trade_amount")
                .message("Trade for Account " + trade.getAccount_number()
                        + " in share class " + trade.getShare_class_id()
                        + " with id " + trade.getTrade_id()
                        + " quantity " + trade.getQuantity()
                        + " over threshold " + quantity
                        + ".")
                .timestamp(new Date()).build();
    }

    private Alert mapNewTradeUnderAmountAlert(final Trade trade, final long amount) {
        return Alert.builder()
                .id(trade.getId())
                .entity_name(trade.getTrade_id())
                .entity_id(trade.getTrade_id())
                .entity_category("trade")
                .event_category("trade_amount")
                .message("Trade for Account " + trade.getAccount_number()
                        + " in share class " + trade.getShare_class_id()
                        + " with id " + trade.getTrade_id()
                        + " amount " + trade.getSettlement_amount()
                        + " over threshold " + amount
                        + ".")
                .timestamp(new Date()).build();
    }

    private Alert mapNewTradeUnderQuantityAlert(final Trade trade, final long quantity) {
        return Alert.builder()
                .id(trade.getId())
                .entity_name(trade.getTrade_id())
                .entity_id(trade.getTrade_id())
                .entity_category("trade")
                .event_category("trade_amount")
                .message("Trade for Account " + trade.getAccount_number()
                        + " in share class " + trade.getShare_class_id()
                        + " with id " + trade.getTrade_id()
                        + " quantity " + trade.getQuantity()
                        + " over threshold " + quantity
                        + ".")
                .timestamp(new Date()).build();
    }

}

