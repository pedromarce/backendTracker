package com.rbc.rbcone.data.rest.kafka.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

public final class JacksonMapperDecorator {

    private static final ObjectMapper mapper = new ObjectMapper();

    private JacksonMapperDecorator() {
    }

    public static <T> T readValue(String string, TypeReference<T> typeReference) {
        try {
            return mapper.readValue(string, typeReference);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static String writeValueAsString(Object value) {
        try {
            return mapper.writeValueAsString(value);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return null;
    }

}
