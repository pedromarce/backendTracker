package com.rbc.rbcone.data.rest.kafka.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.core.type.TypeReference;

import com.rbc.rbcone.data.rest.kafka.util.JacksonMapperDecorator;
import lombok.*;

import java.util.Date;

@NoArgsConstructor
@AllArgsConstructor
@Setter
@Getter
@Builder
@EqualsAndHashCode
@ToString
@JsonIgnoreProperties(ignoreUnknown = true)
public class Trade {

    private String region_id;

    private String account_number;

    private String share_class_id;

    private String trade_id;

    private Double quantity;

    private Double settlement_amount;

    private String status_code;

    private String input_channel;

    private String trade_currency;

    public static Alert mapNewTradeOverAmountAlert (final Trade trade) {
        return Alert.builder()
                .id(trade.region_id + "_" + trade.trade_id)
                .entity_name(trade.trade_id)
                .entity_id(trade.trade_id)
                .entity_category("trade")
                .event_category("trade_amount")
                .message("Trade for Account " + trade.account_number
                        + " in share class " + trade.share_class_id
                        + " with id " + trade.trade_id
                        + " amount " + trade.settlement_amount
                        + ".")
                .timestamp(new Date()).build();
    }

    public static Alert mapNewTradeOverQuantityAlert (final Trade trade) {
        return Alert.builder()
                .id(trade.region_id + "_" + trade.trade_id)
                .entity_name(trade.trade_id)
                .entity_id(trade.trade_id)
                .entity_category("trade")
                .event_category("trade_amoubt")
                .message("Trade for Account " + trade.account_number
                        + " in share class " + trade.share_class_id
                        + " with id " + trade.trade_id
                        + " quantity " + trade.quantity
                        + ".")
                .timestamp(new Date()).build();
    }

    public static Alert mapNewTradeUnderAmountAlert (final Trade trade) {
        return Alert.builder()
                .id(trade.region_id + "_" + trade.trade_id)
                .entity_name(trade.trade_id)
                .entity_id(trade.trade_id)
                .entity_category("trade")
                .event_category("trade_amount")
                .message("Trade for Account " + trade.account_number
                        + " in share class " + trade.share_class_id
                        + " with id " + trade.trade_id
                        + " amount " + trade.settlement_amount
                        + ".")
                .timestamp(new Date()).build();
    }

    public static Alert mapNewTradeUnderQuantityAlert (final Trade trade) {
        return Alert.builder()
                .id(trade.region_id + "_" + trade.trade_id)
                .entity_name(trade.trade_id)
                .entity_id(trade.trade_id)
                .entity_category("trade")
                .event_category("trade_amoubt")
                .message("Trade for Account " + trade.account_number
                        + " in share class " + trade.share_class_id
                        + " with id " + trade.trade_id
                        + " quantity " + trade.quantity
                        + ".")
                .timestamp(new Date()).build();
    }


    public static Trade mapTrade (final String jsonObject) {
        return JacksonMapperDecorator.readValue(jsonObject, new TypeReference<Trade>() {});
    }

}