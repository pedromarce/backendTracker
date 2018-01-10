package com.rbc.rbcone.data.rest.kafka.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.core.type.TypeReference;
import com.rbc.rbcone.data.rest.kafka.util.JacksonMapperDecorator;
import com.rbc.rbcone.data.rest.kafka.util.RandomizeTimeStamp;
import lombok.*;

import java.util.Date;
import java.util.HashMap;
import java.util.Random;

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

    public static Alert mapLiquidatedShareClassAlert (final ShareClass shareClass) {
        return Alert.builder()
                .id(shareClass.region_id + "_" + shareClass.share_class_id)
                .entity_name(shareClass.share_class_name)
                .entity_id(shareClass.share_class_id)
                .entity_category("share_class")
                .event_category("liquidated_share_class")
                .message("Share Class " + shareClass.share_class_id
                        + " with code " + shareClass.share_class_code
                        + " and name " + shareClass.share_class_name
                        + " for fund " + shareClass.legal_fund_id
                        + " has been liquidated.")
                .timestamp(RandomizeTimeStamp.getRandom()).build();
    }


    public static Alert mapNewShareClassAlert (final ShareClass shareClass) {
        return Alert.builder()
                .id(shareClass.region_id + "_" + shareClass.share_class_id)
                .entity_name(shareClass.share_class_name)
                .entity_id(shareClass.share_class_id)
                .entity_category("share_class")
                .event_category("new_share_class")
                .message("New Share Class " + shareClass.share_class_id
                        + " created with code " + shareClass.share_class_code
                        + " and name " + shareClass.share_class_name
                        + " for fund " + shareClass.legal_fund_id
                        + ".")
                .timestamp(RandomizeTimeStamp.getRandom()).build();
    }

    public static TrackerIndex mapTrackerIndex (final ShareClass shareClass) {
        return TrackerIndex.builder()
                .id(shareClass.region_id + "_" + shareClass.share_class_id)
                .entity_id(shareClass.share_class_id)
                .entity_name(shareClass.share_class_name)
                .entity_category("share_class").build();
    }

}
