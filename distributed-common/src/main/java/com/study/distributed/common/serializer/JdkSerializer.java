package com.study.distributed.common.serializer;

import java.io.*;

/**
 * JDK 原生序列化器 - 作为对比参考
 */
public class JdkSerializer implements Serializer {

    @Override
    public byte[] serialize(Object obj) throws Exception {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
             ObjectOutputStream oos = new ObjectOutputStream(bos)) {
            oos.writeObject(obj);
            return bos.toByteArray();
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T deserialize(byte[] data, Class<T> clazz) throws Exception {
        try (ByteArrayInputStream bis = new ByteArrayInputStream(data);
             ObjectInputStream ois = new ObjectInputStream(bis)) {
            return (T) ois.readObject();
        }
    }

    @Override
    public byte getTypeId() {
        return 0;
    }
}
