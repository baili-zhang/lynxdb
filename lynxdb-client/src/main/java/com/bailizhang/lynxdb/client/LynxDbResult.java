package com.bailizhang.lynxdb.client;

import com.bailizhang.lynxdb.client.utils.LynxDbUtils;
import com.bailizhang.lynxdb.core.utils.BufferUtils;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import static com.bailizhang.lynxdb.server.engine.result.Result.*;

/**
 * 解析不同结构的返回数据
 */
public class LynxDbResult {
    private final ByteBuffer buffer;
    private final byte status;
    private String message;

    public LynxDbResult(byte[] value) {
        buffer = ByteBuffer.wrap(value);
        status = buffer.get();
    }

    public boolean isSuccessful() {
        if(status == SUCCESS ||
                status == SUCCESS_WITH_LIST ||
                status == SUCCESS_WITH_KV_PAIRS ||
                status == SUCCESS_WITH_TABLE) {
            return true;
        }

        message = BufferUtils.getRemainingString(buffer);
        return false;
    }

    public String message() {
        return message;
    }

    public <T> T kvGet(Class<T> clazz) {
        if(status != SUCCESS_WITH_KV_PAIRS) {
            throw new UnsupportedOperationException();
        }

        T instance;

        try {
            instance = clazz.getDeclaredConstructor().newInstance();
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
            throw new RuntimeException(e);
        }

        String name = clazz.getName();
        ConcurrentHashMap<String, Field> fields = LynxDbUtils.findKvFieldMap(name);
        if(fields == null) {
            throw new RuntimeException("No define found");
        }

        while (BufferUtils.isNotOver(buffer)) {
            String key = BufferUtils.getString(buffer);
            Field field = fields.get(key);

            Object value = BufferUtils.getByType(buffer, field.getType());
            try {
                field.set(instance, value);
            } catch (IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        }

        return instance;
    }

    public List<String> listGet() {
        if (status != SUCCESS_WITH_LIST) {
            throw new UnsupportedOperationException();
        }

        List<String> list = new ArrayList<>();

        while (BufferUtils.isNotOver(buffer)) {
            list.add(BufferUtils.getString(buffer));
        }

        return list;
    }

    public <T> List<T> tableGet(Class<T> clazz) {
        throw new UnsupportedOperationException();
    }
}
