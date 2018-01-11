package com.rbc.rbcone.data.rest.kafka.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.core.type.TypeReference;

import com.rbc.rbcone.data.rest.kafka.dto.elastic.TrackerIndex;
import com.rbc.rbcone.data.rest.kafka.util.JacksonMapperDecorator;
import lombok.*;

import java.util.Map;

@NoArgsConstructor
@AllArgsConstructor
@Setter
@Getter
@Builder
@EqualsAndHashCode
@ToString
@JsonIgnoreProperties(ignoreUnknown = true)
public class Account {

    private String region_id;

    private String account_number;

    private String account_name;

    private String dealer_id;

    public String getId() {
        return region_id + "_" + account_number;
    }

    public String getDealer_id() {
        return region_id + "_" + account_number.substring(0,2);
    }

    public static Account mapAccount (final String jsonObject) {
        System.out.println("Process Account");
        return JacksonMapperDecorator.readValue(jsonObject, new TypeReference<Account>() {});
    }

    public static TrackerIndex mapTrackerIndex (final Account account) {
        return TrackerIndex.builder()
                .id(account.getId())
                .entity_id(account.account_number)
                .entity_name(account.account_name)
                .entity_category("Account").build();
    }

    public Map<String, Object> toMap() {
        return JacksonMapperDecorator.writeAsMap(this);
    }
}