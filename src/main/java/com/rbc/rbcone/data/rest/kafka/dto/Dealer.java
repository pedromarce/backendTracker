package com.rbc.rbcone.data.rest.kafka.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.core.type.TypeReference;

import com.rbc.rbcone.data.rest.kafka.dto.elastic.TrackerIndex;
import com.rbc.rbcone.data.rest.kafka.dto.firebase.Alert;
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

    public String getId() {
        return region_id + "_" + dealer_id;
    }

    public static Dealer mapDealer (final String jsonObject) {
        return JacksonMapperDecorator.readValue(jsonObject, new TypeReference<Dealer>() {});
    }

    public static TrackerIndex mapTrackerIndex (final Dealer dealer) {
        return TrackerIndex.builder()
                .id(dealer.getId())
                .entity_id(dealer.dealer_id)
                .entity_name(dealer.dealer_name)
                .entity_category("dealer").build();
    }

    public String toJson() {
        return JacksonMapperDecorator.writeValueAsString(this);
    }
}