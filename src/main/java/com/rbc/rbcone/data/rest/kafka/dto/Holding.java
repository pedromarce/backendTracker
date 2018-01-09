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
public class Holding {

    private String region_id;

    private String account_number;

    private String share_class_id;

    private Double quantity;

    private Double amount;

    private String is_inactive;

    private String is_blocked;

    private String is_closed;

    private String blocking_reason_code;


    public static Holding mapHolding (final String jsonObject) {
        return JacksonMapperDecorator.readValue(jsonObject, new TypeReference<Holding>() {});
    }

}