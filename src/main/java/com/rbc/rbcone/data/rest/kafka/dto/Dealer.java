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
public class Dealer {

    private String region_id;

    private String dealer_id;

    private String dealer_name;

    public static Dealer mapDealer (final String jsonObject) {
        return JacksonMapperDecorator.readValue(jsonObject, new TypeReference<Dealer>() {});
    }


    public static TrackerIndex mapTrackerIndex (final Dealer dealer) {
        return TrackerIndex.builder()
                .entity(dealer.dealer_id)
                .entity_name(dealer.dealer_name)
                .entity_category("dealer").build();
    }

}