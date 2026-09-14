package com.study.distributed.kv.store;

import java.util.Map;

/**
 * KV 存储接口
 */
public interface KVStore {

    String get(String key);

    void put(String key, String value);

    String delete(String key);

    Map<String, String> getAll();

    int size();
}
