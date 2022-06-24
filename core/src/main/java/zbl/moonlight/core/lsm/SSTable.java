package zbl.moonlight.core.lsm;

import zbl.moonlight.core.enhance.EnhanceByteBuffer;
import zbl.moonlight.core.enhance.EnhanceFile;
import zbl.moonlight.core.utils.NumberUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;

import static zbl.moonlight.core.utils.NumberUtils.BYTE_LENGTH;
import static zbl.moonlight.core.utils.NumberUtils.INT_LENGTH;

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

    private final BloomFilter bloomFilter;
    private final int capacity;
    private final EnhanceFile file;

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
        file = new EnhanceFile(DEFAULT_DATE_LOG_DIR, String.format(DATA_SLICE_NAME_TEMPLATE, level, index));

        sizePosition = (long) capacity * MAGNIFICATION + BLOOM_FILTER_POSITION;
        entryPosition = sizePosition + NumberUtils.INT_LENGTH;

        /* 布隆过滤器的长度 */
        int len = (int)sizePosition - BLOOM_FILTER_POSITION;
        ByteBuffer buffer = file.read(BLOOM_FILTER_POSITION, len);
        /* 如果布隆过滤器没有创建 */
        if(!EnhanceByteBuffer.isOver(buffer)) {
            if(buffer.position() == 0) {
                file.write(ByteBuffer.allocate(len), BLOOM_FILTER_POSITION);
                file.writeInt(INIT_SIZE, sizePosition);
            } else {
                throw new RuntimeException("Incomplete bloom filter.");
            }
        }

        size = file.readInt(sizePosition);
        bloomFilter = new BloomFilter(file, capacity << 3);
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
        if(!(key instanceof String str)) {
            throw new RuntimeException("[key] is not an instance of [String].");
        }

        return bloomFilter.isKeyExist(str);
    }

    @Override
    public boolean containsValue(Object value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public byte[] get(Object key) {
        if(!(key instanceof String str)) {
            throw new RuntimeException("[key] is not an instance of [String].");
        }

        /* 如果布隆过滤器中不存在，则返回 null */
        if(!bloomFilter.isKeyExist(str)) {
            return null;
        }

        long position = entryPosition;
        SSTableEntry finalEntry = null;
        for (int i = 0; i < size; i++) {
            try {
                SSTableEntry entry = new SSTableEntry(file.readBytes(position));
                if(entry.isKey(str)) {
                    finalEntry = entry;
                }
                /* 更新 position */
                position += entry.totalLength();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        return finalEntry == null || finalEntry.isDelete() ? null : finalEntry.value();
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
        if(value == null) {
            value = new byte[0];
        }

        /* 布隆过滤器设置 key 值 */
        bloomFilter.setKey(keyStr);

        byte[] key = keyStr.getBytes(StandardCharsets.UTF_8);

        int len = BYTE_LENGTH + INT_LENGTH + key.length + value.length;
        ByteBuffer buffer = ByteBuffer.allocate(len + INT_LENGTH);
        buffer.putInt(len).put(status).putInt(key.length).put(key).put(value);

        try {
            file.append(buffer.array());
            size ++;
            file.writeInt(size, sizePosition);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void putAll(Map<? extends String, ? extends byte[]> m) {
        for(String key : m.keySet()) {
            put(key, m.get(key));
        }
    }

    @Override
    public void clear() {
        try {
            file.delete();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public Set<String> keySet() {
        HashSet<String> keySet = new HashSet<>();

        long position = entryPosition;
        for (int i = 0; i < size; i++) {
            try {
                SSTableEntry entry = new SSTableEntry(file.readBytes(position));
                String key = entry.key();

                if(entry.isDelete()) {
                    keySet.remove(key);
                } else {
                    keySet.add(key);
                }
                /* 更新 position */
                position += entry.totalLength();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        return keySet;
    }

    @Override
    public Collection<byte[]> values() {
        HashMap<String, byte[]> map = new HashMap<>();

        long position = entryPosition;
        for (int i = 0; i < size; i++) {
            try {
                SSTableEntry entry = new SSTableEntry(file.readBytes(position));
                String key = entry.key();
                byte[] value = entry.value();

                if(entry.isDelete()) {
                    map.remove(key);
                } else {
                    map.put(key, value);
                }
                /* 更新 position */
                position += entry.totalLength();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        return map.values();
    }

    @Override
    public Set<Entry<String, byte[]>> entrySet() {
        HashMap<String, byte[]> map = new HashMap<>();

        long position = entryPosition;
        for (int i = 0; i < size; i++) {
            try {
                SSTableEntry entry = new SSTableEntry(file.readBytes(position));
                String key = entry.key();
                byte[] value = entry.value();

                if(entry.isDelete()) {
                    map.remove(key);
                } else {
                    map.put(key, value);
                }
                /* 更新 position */
                position += entry.totalLength();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        return map.entrySet();
    }

    public boolean isFull() {
        return size == capacity;
    }

    public void close() throws IOException {
        file.close();
    }
}
