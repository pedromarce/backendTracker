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
public class ShareClass {

    private String region_id;

    private String share_class_id;

    private String share_class_name;

    private Boolean is_liquidated;

    private String legal_fund_id;

    private String share_class_code;

    public String getId() {
        return region_id + "_" + share_class_id;
    }

    public static ShareClass mapShareClass (final String jsonObject) {
        System.out.println("Process ShareClass");
        return JacksonMapperDecorator.readValue(jsonObject, new TypeReference<ShareClass>() {});
    }

    public static TrackerIndex mapTrackerIndex (final ShareClass shareClass) {
        return TrackerIndex.builder()
                .id(shareClass.region_id + "_" + shareClass.share_class_id)
                .entity_id(shareClass.share_class_id)
                .entity_name(shareClass.share_class_name)
                .entity_category("share_class").build();
    }

    public Map<String, Object> toMap() {
        return JacksonMapperDecorator.writeAsMap(this);
    }}
