package com.study.distributed.common.serializer;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

/**
 * JSON 序列化器 - 基于 Jackson，可读性好，方便调试学习
 */
public class JsonSerializer implements Serializer {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    static {
        MAPPER.registerModule(new JavaTimeModule());
        MAPPER.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        MAPPER.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        MAPPER.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
    }

    @Override
    public byte[] serialize(Object obj) throws Exception {
        return MAPPER.writeValueAsBytes(obj);
    }

    @Override
    public <T> T deserialize(byte[] data, Class<T> clazz) throws Exception {
        return MAPPER.readValue(data, clazz);
    }

    @Override
    public byte getTypeId() {
        return 1;
    }

    public static ObjectMapper getMapper() {
        return MAPPER;
    }
}
