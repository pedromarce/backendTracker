package com.rbc.rbcone.data.rest.kafka.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.core.type.TypeReference;

import com.rbc.rbcone.data.rest.kafka.util.JacksonMapperDecorator;
import lombok.*;

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


    public static Trade mapTrade (final String jsonObject) {
        return JacksonMapperDecorator.readValue(jsonObject, new TypeReference<Trade>() {});
    }

}