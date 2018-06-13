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
public class LegalFund {

    private String region_id;

    private String legal_fund_id;

    private String legal_fund_name;

    public String getId() {
        return region_id + "_" + legal_fund_id;
    }

    public static LegalFund mapLegalFund (final String jsonObject) {
        System.out.println("Process LegalFund");
        return JacksonMapperDecorator.readValue(jsonObject, new TypeReference<LegalFund>() {});
    }

    public static TrackerIndex mapTrackerIndex (final LegalFund legalFund) {
        return TrackerIndex.builder()
                .id(legalFund.getId())
                .entity_id(legalFund.legal_fund_id)
                .entity_name(legalFund.legal_fund_name)
                .entity_category("legal_fund").build();
    }

    public Map<String, Object> toMap() {
        return JacksonMapperDecorator.writeAsMap(this);
    }
}
