package com.rbc.rbcone.data.rest.kafka.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.core.type.TypeReference;

import com.rbc.rbcone.data.rest.kafka.dto.elastic.TrackerIndex;
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
public class Account {

    private String region_id;

    private String account_number;

    private String account_name;

    public String getId() {
        return region_id + "_" + account_number;
    }

    public static Account mapAccount (final String jsonObject) {
        return JacksonMapperDecorator.readValue(jsonObject, new TypeReference<Account>() {});
    }

    public static TrackerIndex mapTrackerIndex (final Account account) {
        return TrackerIndex.builder()
                .id(account.getId())
                .entity_id(account.account_number)
                .entity_name(account.account_name)
                .entity_category("Account").build();
    }

    public String toJson() {
        return JacksonMapperDecorator.writeValueAsString(this);
    }
}