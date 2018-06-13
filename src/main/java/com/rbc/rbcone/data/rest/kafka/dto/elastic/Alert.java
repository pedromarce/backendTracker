package com.rbc.rbcone.data.rest.kafka.dto.elastic;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.rbc.rbcone.data.rest.kafka.util.JacksonMapperDecorator;
import lombok.*;

import java.util.Date;
import java.util.Map;

@NoArgsConstructor
@AllArgsConstructor
@Setter
@Getter
@Builder
@EqualsAndHashCode
@ToString
@JsonIgnoreProperties(ignoreUnknown = true)
public class Alert {

    private String id;

    private String entity_name;

    private String entity_id;

    private String entity_category;

    private String event_category;

    private String message;

    private Date timestamp;

    public Map<String, Object> toMap() {
        return JacksonMapperDecorator.writeAsMap(this);
    }

}
