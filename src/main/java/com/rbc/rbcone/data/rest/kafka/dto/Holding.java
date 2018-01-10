package com.rbc.rbcone.data.rest.kafka.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.core.type.TypeReference;

import com.rbc.rbcone.data.rest.kafka.util.JacksonMapperDecorator;
import com.rbc.rbcone.data.rest.kafka.util.RandomizeTimeStamp;
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
public class Holding {

    private String region_id;

    private String account_number;

    private String share_class_id;

    private Double quantity;

    private Double amount;

    private Boolean is_inactive;

    private Boolean is_blocked;

    private Boolean is_closed;

    private String blocking_reason_code;

    public static Alert mapBlockHoldingShareClassAlert (final Holding holding) {
        return Alert.builder()
                .id(holding.region_id + "_" + holding.share_class_id)
                .entity_name(holding.share_class_id)
                .entity_id(holding.share_class_id)
                .entity_category("share_class")
                .event_category("holding_blocked")
                .message("Holding for Share Class " + holding.share_class_id
                        + " in account " + holding.account_number
                        + " has been blocked due to " + holding.blocking_reason_code
                        + ".")
                .timestamp(RandomizeTimeStamp.getRandom()).build();
    }

    public static Alert mapBlockHoldingAccountAlert (final Holding holding) {
        return Alert.builder()
                .id(holding.region_id + "_" + holding.account_number)
                .entity_name(holding.account_number)
                .entity_id(holding.account_number)
                .entity_category("account")
                .event_category("holding_blocked")
                .message("Holding for Account " + holding.account_number
                        + " in share class " + holding.share_class_id
                        + " has been blocked due to " + holding.blocking_reason_code
                        + ".")
                .timestamp(RandomizeTimeStamp.getRandom()).build();
    }

    public static Holding mapHolding (final String jsonObject) {
        return JacksonMapperDecorator.readValue(jsonObject, new TypeReference<Holding>() {});
    }

}