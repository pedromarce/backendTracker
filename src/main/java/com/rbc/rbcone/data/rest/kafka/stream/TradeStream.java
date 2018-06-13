package com.rbc.rbcone.data.rest.kafka.stream;

import com.rbc.rbcone.data.rest.kafka.dto.LegalFund;
import com.rbc.rbcone.data.rest.kafka.dto.Trade;
import com.rbc.rbcone.data.rest.kafka.dto.elastic.Alert;
import com.rbc.rbcone.data.rest.kafka.util.ElasticSearchService;
import com.rbc.rbcone.data.rest.kafka.util.KafkaProducerInstance;
import com.rbc.rbcone.data.rest.kafka.util.RandomizeTimeStamp;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.UUID;

@Component("TradeStream")
public class TradeStream {

    private StreamsBuilder streamsBuilder;

    private ElasticSearchService elasticSearchService;

    private KafkaProducerInstance kafkaProducerInstance;

    public TradeStream(StreamsBuilder streamsBuilder, ElasticSearchService elasticSearchService, KafkaProducerInstance kafkaProducerInstance) {
        this.streamsBuilder = streamsBuilder;
        this.elasticSearchService = elasticSearchService;
        this.kafkaProducerInstance = kafkaProducerInstance;
        buildFirebaseViewStoreStreams();
    }

    private void buildFirebaseViewStoreStreams() {

        final KTable<String, LegalFund> tableShareClass = streamsBuilder.table("table_shareClass");

        final KStream<String, String> tradeStream = streamsBuilder.stream("replica_trade");
        tradeStream
                .to("kafka_process");
        tradeStream
                .mapValues(Trade::mapTrade)
                .selectKey((key, value) -> value.getId())
                .join(tableShareClass, (trade, shareClass) -> { trade.setLegal_fund_id(shareClass.getLegal_fund_id()); return trade;})
                .filter(this::filterNonNull)
                .mapValues(this::indexTrade)
                .mapValues(this::sendTradeAlerts);

    }

    private boolean filterNonNull(String key, Trade trade) {
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

        try {
            if (trade.getStatus_code() != "C" && trade.getStatus_code() != "H") {
                if (trade.getSettlement_amount() != 0 && trade.getSettlement_amount() > 500000) {
                    elasticSearchService.index("alerts", UUID.randomUUID().toString(), mapNewTradeOverAmountAlert(trade, 500000).toMap());
                    kafkaProducerInstance.getProducer().send(new ProducerRecord<String, String>("alert", "alert_trade", "{}"));
                    System.out.println("Sent alert Trade");
                }
                if (trade.getQuantity() != 0 && trade.getQuantity() > 15000) {
                    elasticSearchService.index("alerts", UUID.randomUUID().toString(), mapNewTradeOverQuantityAlert(trade, 15000).toMap());
                    kafkaProducerInstance.getProducer().send(new ProducerRecord<String, String>("alert", "alert_trade", "{}"));
                    System.out.println("Sent alert Trade");
                }

                if (trade.getSettlement_amount() != 0 && trade.getSettlement_amount() < 0.1) {
                    elasticSearchService.index("alerts", UUID.randomUUID().toString(), mapNewTradeUnderAmountAlert(trade, 100).toMap());
                    kafkaProducerInstance.getProducer().send(new ProducerRecord<String, String>("alert", "alert_trade", "{}"));
                    System.out.println("Sent alert Trade");
                }
                if (trade.getQuantity() != 0 && trade.getQuantity() < 0.005) {
                    elasticSearchService.index("alerts", UUID.randomUUID().toString(), mapNewTradeUnderQuantityAlert(trade, 1).toMap());
                    kafkaProducerInstance.getProducer().send(new ProducerRecord<String, String>("alert", "alert_trade", "{}"));
                    System.out.println("Sent alert Trade");
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
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

