package com.study.distributed.kv.store;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 内存 KV 存储
 */
public class MemoryKVStore implements KVStore {

    private final Map<String, String> data = new ConcurrentHashMap<>();

    @Override
    public String get(String key) {
        return data.get(key);
    }

    @Override
    public void put(String key, String value) {
        data.put(key, value);
    }

    @Override
    public String delete(String key) {
        return data.remove(key);
    }

    @Override
    public Map<String, String> getAll() {
        return Collections.unmodifiableMap(data);
    }

    @Override
    public int size() {
        return data.size();
    }
}
