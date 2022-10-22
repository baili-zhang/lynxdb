package com.bailizhang.lynxdb.client.utils;

import com.bailizhang.lynxdb.client.annotation.LynxDbColumn;
import com.bailizhang.lynxdb.client.annotation.LynxDbKey;
import com.bailizhang.lynxdb.client.annotation.LynxDbKvstore;
import com.bailizhang.lynxdb.client.annotation.LynxDbTable;
import com.bailizhang.lynxdb.core.common.G;
import com.bailizhang.lynxdb.core.utils.BufferUtils;
import com.bailizhang.lynxdb.core.utils.ClassUtils;
import com.bailizhang.lynxdb.storage.core.Pair;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public abstract class LynxDbUtils {
    private static final ConcurrentHashMap<String, ConcurrentHashMap<String, Field>> kvDefineMap
            = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<String, ConcurrentHashMap<String, Field>> tableDefineMap
            = new ConcurrentHashMap<>();

    public static Collection<Field> findFields(Class<?> clazz) {
        LynxDbKvstore kvstore = clazz.getAnnotation(LynxDbKvstore.class);
        LynxDbTable table = clazz.getAnnotation(LynxDbTable.class);

        if(kvstore != null && table != null) {
            throw new RuntimeException("Annotation \"LynxDbKvstore\" can not use with \"LynxDbTable\"");
        }

        if(kvstore == null && table == null) {
            throw new RuntimeException("Can not find annotation \"LynxDbKvstore\" or \"LynxDbTable\"");
        }

        return kvstore != null
                ? loadFields(clazz, LynxDbKey.class, kvDefineMap)
                : loadFields(clazz, LynxDbColumn.class, tableDefineMap);
    }

    public static ConcurrentHashMap<String, Field> findKvFieldMap(String name) {
        return kvDefineMap.get(name);
    }

    public static String findTableName(Class<?> clazz) {
        LynxDbTable lynxDbTable = clazz.getAnnotation(LynxDbTable.class);
        if(lynxDbTable == null) {
            throw new RuntimeException("Not found \"LynxDbKvstore\"");
        }
        return lynxDbTable.value();
    }

    public static String findKvstoreName(Class<?> clazz) {
        LynxDbKvstore lynxDbKvstore = clazz.getAnnotation(LynxDbKvstore.class);
        if(lynxDbKvstore == null) {
            throw new RuntimeException("Not found \"LynxDbKvstore\"");
        }
        return lynxDbKvstore.value();
    }

    public static List<Pair<byte[],byte[]>> findKvPairs(Object o) {
        List<Pair<byte[], byte[]>> kvPairs = new ArrayList<>();
        Collection<Field> fields = findFields(o.getClass());

        fields.forEach(field -> {
            String key = field.getName();
            Object val;

            try {
                val = field.get(o);
            } catch (IllegalAccessException e) {
                throw new RuntimeException(e);
            }

            byte[] keyBytes = G.I.toBytes(key);
            byte[] valueBytes = BufferUtils.toBytes(val);
            kvPairs.add(new Pair<>(keyBytes, valueBytes));
        });

        return kvPairs;
    }

    private static Collection<Field> loadFields(Class<?> clazz, Class<? extends Annotation> annotation,
                                                ConcurrentHashMap<String, ConcurrentHashMap<String, Field>> defineMap) {
        String name = clazz.getName();
        ConcurrentHashMap<String, Field> fieldMap = tableDefineMap.get(name);

        if(fieldMap == null) {

            ConcurrentHashMap<String, Field> newFieldMap = new ConcurrentHashMap<>();
            List<Field> fields = ClassUtils.findFields(clazz, annotation);

            fields.forEach(field -> {
                field.setAccessible(true);
                newFieldMap.put(field.getName(), field);
            });

            defineMap.put(name, newFieldMap);
            return fields;
        }

        return fieldMap.values();
    }
}
