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
                .timestamp(new Date()).build();
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
                .timestamp(new Date()).build();
    }

    public static Alert mapInactiveHoldingShareClassAlert (final Holding holding) {
        return Alert.builder()
                .id(holding.region_id + "_" + holding.share_class_id)
                .entity_name(holding.share_class_id)
                .entity_id(holding.share_class_id)
                .entity_category("share_class")
                .event_category("holding_inactive")
                .message("Holding for Account " + holding.account_number
                        + " in share class " + holding.share_class_id
                        + " has been inactive due to " + holding.blocking_reason_code
                        + ".")
                .timestamp(new Date()).build();
    }

    public static Alert mapInactiveHoldingAccountAlert (final Holding holding) {
        return Alert.builder()
                .id(holding.region_id + "_" + holding.account_number)
                .entity_name(holding.account_number)
                .entity_id(holding.account_number)
                .entity_category("account")
                .event_category("holding_inactive")
                .message("Holding for Account " + holding.account_number
                        + " in share class " + holding.share_class_id
                        + " has been inactive due to " + holding.blocking_reason_code
                        + ".")
                .timestamp(new Date()).build();
    }

    public static Alert mapBalanceHoldingAccountAlert (final Holding holding) {
        return Alert.builder()
                .id(holding.region_id + "_" + holding.account_number)
                .entity_name(holding.account_number)
                .entity_id(holding.account_number)
                .entity_category("account")
                .event_category("holding_balance")
                .message("Holding for Account " + holding.account_number
                        + " in share class " + holding.share_class_id
                        + " is " + holding.quantity
                        + ".")
                .timestamp(new Date()).build();
    }

    public static Alert mapBalanceHoldingClassAlert (final Holding holding) {
        return Alert.builder()
                .id(holding.region_id + "_" + holding.share_class_id)
                .entity_name(holding.share_class_id)
                .entity_id(holding.share_class_id)
                .entity_category("share_class")
                .event_category("holding_balance")
                .message("Holding for Account " + holding.account_number
                        + " in share class " + holding.share_class_id
                        + " exceeds 50% of the share class"
                        + ".")
                .timestamp(new Date()).build();
    }

    public static Alert mapNewHoldingDealerAlert (final Holding holding) {
        return Alert.builder()
                .id(holding.region_id + "_" + holding.account_number)  /* add dealer id or substring */
                .entity_name(holding.account_number)
                .entity_id(holding.account_number)
                .entity_category("dealer")
                .event_category("new_holding")
                .message("New Holding "
                        + " created with share class " + holding.share_class_id
                        + " and account " + holding.account_number
                        + " for dealer " + holding.account_number
                        + ".")
                .timestamp(new Date()).build();
    }

    public static Holding mapHolding (final String jsonObject) {
        return JacksonMapperDecorator.readValue(jsonObject, new TypeReference<Holding>() {});
    }

}