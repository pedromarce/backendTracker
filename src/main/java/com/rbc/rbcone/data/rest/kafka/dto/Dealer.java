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
public class Dealer {

    private String region_id;

    private String dealer_id;

    private String dealer_name;

    public static Dealer mapDealer (final String jsonObject) {
        return JacksonMapperDecorator.readValue(jsonObject, new TypeReference<Dealer>() {});
    }

    public static Alert mapBalanceDealerAlert (final Dealer dealer ) {
        return Alert.builder()
                .id(dealer.region_id + "_" + dealer.dealer_id)
                .entity_name(dealer.dealer_name)
                .entity_id(dealer.dealer_id)
                .entity_category("dealer")
                .event_category("dealer_balance")
                .message("Balance for dealer " + dealer.dealer_id
                        + ".")
                .timestamp(new Date()).build();
    }

    public static TrackerIndex mapTrackerIndex (final Dealer dealer) {
        return TrackerIndex.builder()
                .id(dealer.region_id + "_" + dealer.dealer_id)
                .entity_id(dealer.dealer_id)
                .entity_name(dealer.dealer_name)
                .entity_category("dealer").build();
    }

}