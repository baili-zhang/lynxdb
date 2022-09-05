package com.bailizhang.lynxdb.core.lsm;

import com.bailizhang.lynxdb.core.file.LogFile;
import com.bailizhang.lynxdb.core.utils.PrimitiveTypeUtils;

import java.io.IOException;
import java.util.*;

/**
 * DataSlice 最小时：size = 1000, 布隆过滤器的长度 = 10000
 *
 * level = 1, capacity = 1000
 * level = 2, capacity = 10000
 * ......
 * level = n, capacity = 1000 * (10 ^ n)
 *
 * 文件格式：
 *
 * | bloom filter | size | entry | ... |
 * |--------------|------|-------|-----|
 * |              |      |       |     |
 */
public class SSTable implements Map<String, byte[]> {
    private static final String DEFAULT_DATE_LOG_DIR = System.getProperty("user.dir") + "/data";
    private static final String DATA_SLICE_NAME_TEMPLATE = "slice-%d-%d.data";

    private static final int BLOOM_FILTER_POSITION = 0;
    private static final int INIT_SIZE = 0;

    /**
     * 默认的最小容量
     */
    private static final int DEFAULT_MIN_SIZE = 1000;
    /**
     * 倍率
     */
    private static final int MAGNIFICATION = 10;

    private final long sizePosition;
    private final long entryPosition;

    private final int capacity;
    private final LogFile file;

    private int size;

    public SSTable(int level, int index) throws IOException {
        /* level 必须大于 0 */
        if(level <= 0) {
            throw new RuntimeException("level can not less than \"0\" or equals \"0\".");
        }

        /* index 必须大于等于 0 */
        if(index < 0) {
            throw new RuntimeException("index can not less than \"0\".");
        }


        capacity = (int) Math.pow(MAGNIFICATION, level - 1) * DEFAULT_MIN_SIZE;
        file = new LogFile(DEFAULT_DATE_LOG_DIR, String.format(DATA_SLICE_NAME_TEMPLATE, level, index));

        sizePosition = (long) capacity * MAGNIFICATION + BLOOM_FILTER_POSITION;
        entryPosition = sizePosition + PrimitiveTypeUtils.INT_LENGTH;

        /* 布隆过滤器的长度 */
        int len = (int)sizePosition - BLOOM_FILTER_POSITION;
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public boolean isEmpty() {
        return size == 0;
    }

    @Override
    public boolean containsKey(Object key) {
        return false;
    }

    @Override
    public boolean containsValue(Object value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public byte[] get(Object key) {
        return null;
    }

    @Override
    public byte[] put(String key, byte[] value) {
        append(SSTableEntry.SET_FLAG, key, value);
        /* 因为不是原地更新，所以直接返回 null */
        return null;
    }

    @Override
    public byte[] remove(Object key) {
        if(!(key instanceof String str)) {
            throw new RuntimeException("[key] is not an instance of [String].");
        }

        appendDelete(str);
        /* 因为不是原地更新，所以直接返回 null */
        return null;
    }

    private void appendDelete(String key) {
        append(SSTableEntry.DELETE_FLAG, key, null);
    }

    private void append(byte status, String keyStr, byte[] value) {
    }

    @Override
    public void putAll(Map<? extends String, ? extends byte[]> m) {
        for(String key : m.keySet()) {
            put(key, m.get(key));
        }
    }

    @Override
    public void clear() {

    }

    @Override
    public Set<String> keySet() {
        return new HashSet<>();
    }

    @Override
    public Collection<byte[]> values() {
        return new ArrayList<>();
    }

    @Override
    public Set<Entry<String, byte[]>> entrySet() {
        return new HashSet<>();
    }

    public boolean isFull() {
        return size == capacity;
    }

    public void close() throws IOException {
        file.close();
    }
}
