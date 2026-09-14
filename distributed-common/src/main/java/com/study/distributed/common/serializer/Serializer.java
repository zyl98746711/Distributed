package com.study.distributed.common.serializer;

/**
 * 序列化器接口 - 可插拔的序列化策略
 */
public interface Serializer {

    /**
     * 序列化
     */
    byte[] serialize(Object obj) throws Exception;

    /**
     * 反序列化
     */
    <T> T deserialize(byte[] data, Class<T> clazz) throws Exception;

    /**
     * 序列化方式标识
     */
    byte getTypeId();
}
