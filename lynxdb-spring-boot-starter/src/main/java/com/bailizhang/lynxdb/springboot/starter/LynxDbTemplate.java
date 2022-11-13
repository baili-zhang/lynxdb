package com.bailizhang.lynxdb.springboot.starter;

import com.bailizhang.lynxdb.core.utils.BufferUtils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class LynxDbTemplate extends AsyncLynxDbTemplate {
    public LynxDbTemplate(LynxDbProperties properties) {
        super(properties);
    }

    public <T> T kvGet(Class<T> clazz) {
        return client.kvGet(current, clazz);
    }

    public void kvSet(Object o) {
        client.kvSet(current, o);
    }

    public void kvValueListInsert(String kvstore, String key, List<String> values) {
        client.kvValueListInsert(current, kvstore, key, values);
    }

    public List<String> kvValueListGet(String kvstore, String key) {
        byte[] value = client.kvGet(current, kvstore, List.of(key));
        List<String> valueList = new ArrayList<>();

        ByteBuffer buffer = ByteBuffer.wrap(value);
        while (BufferUtils.isNotOver(buffer)) {
            String valueItem = BufferUtils.getString(buffer);
            valueList.add(valueItem);
        }

        return valueList;
    }

    public void kvValueListRemove(String kvstore, String key, List<String> values) {
        client.kvValueListRemove(current, kvstore, key, values);
    }
}
