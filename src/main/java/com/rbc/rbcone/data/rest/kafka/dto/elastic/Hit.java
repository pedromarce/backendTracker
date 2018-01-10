package com.rbc.rbcone.data.rest.kafka.dto.elastic;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.*;

@NoArgsConstructor
@AllArgsConstructor
@Setter
@Getter
@Builder
@EqualsAndHashCode
@ToString
@JsonIgnoreProperties(ignoreUnknown = true)
public class Hit {

    private String _index;

    private String _type;

    private String _id;

    private boolean found;

    private Object _source;

}
