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
public class LegalFund {

    private String region_id;

    private String legal_fund_id;

    private String legal_fund_name;

    public static LegalFund mapLegalFund (final String jsonObject) {
        return JacksonMapperDecorator.readValue(jsonObject, new TypeReference<LegalFund>() {});
    }

    public static Alert mapNewLegalFundAlert (final LegalFund legalFund) {
        return Alert.builder()
                .id(legalFund.region_id + "_" + legalFund.legal_fund_id)
                .entity_name(legalFund.legal_fund_name)
                .entity_id(legalFund.legal_fund_id)
                .entity_category("legal_fund")
                .event_category("new_legal_fund")
                .message("New Legal Fund "
                        + " create with code " + legalFund.legal_fund_id
                        + " and name " + legalFund.legal_fund_name
                        + ".")
                .timestamp(new Date()).build();
    }

    public static TrackerIndex mapTrackerIndex (final LegalFund legalFund) {
        return TrackerIndex.builder()
                .id(legalFund.region_id + "_" + legalFund.legal_fund_id)
                .entity_id(legalFund.legal_fund_id)
                .entity_name(legalFund.legal_fund_name)
                .entity_category("legal_fund").build();
    }

}
