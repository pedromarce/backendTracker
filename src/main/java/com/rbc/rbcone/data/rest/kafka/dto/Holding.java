package com.rbc.rbcone.data.rest.kafka.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.core.type.TypeReference;

import com.rbc.rbcone.data.rest.kafka.dto.firebase.Alert;
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

    public String getId() {
        return region_id + "_" + account_number + "_" + share_class_id;
    }

    public static Holding mapHolding (final String jsonObject) {
        return JacksonMapperDecorator.readValue(jsonObject, new TypeReference<Holding>() {});
    }

    public String toJson() {
        return JacksonMapperDecorator.writeValueAsString(this);
    }
}