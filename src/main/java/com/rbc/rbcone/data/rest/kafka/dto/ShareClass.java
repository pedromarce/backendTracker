package com.rbc.rbcone.data.rest.kafka.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.core.type.TypeReference;
import com.rbc.rbcone.data.rest.kafka.util.JacksonMapperDecorator;
import lombok.*;

import java.util.Date;
import java.util.HashMap;

@NoArgsConstructor
@AllArgsConstructor
@Setter
@Getter
@Builder
@EqualsAndHashCode
@ToString
@JsonIgnoreProperties(ignoreUnknown = true)
public class ShareClass {

    private String region_id;

    private String share_class_id;

    private String share_class_name;

    private Boolean is_liquidated;

    private String legal_fund_id;

    private String share_class_code;

    public static ShareClass mapShareClass (final String jsonObject) {
        return JacksonMapperDecorator.readValue(jsonObject, new TypeReference<ShareClass>() {});
    }

    public static Alert mapNewShareClassAlert (final ShareClass shareClass) {
        return Alert.builder()
                .entity(shareClass.share_class_name)
                .entityId(shareClass.share_class_id)
                .entityCategory("share_class")
                .eventCategory("new_share_class")
                .message("New Share " + shareClass.share_class_id
                        + " create with code " + shareClass.share_class_code
                        + " and name " + shareClass.share_class_name
                        + " for fund " + shareClass.legal_fund_id
                        + ".")
                .timestamp(new Date()).build();
    }

    public static TrackerIndex mapTrackerIndex (final ShareClass shareClass) {
        return TrackerIndex.builder()
                .entity(shareClass.share_class_id)
                .entity_name(shareClass.share_class_name)
                .entity_category("share_class").build();
    }

}
