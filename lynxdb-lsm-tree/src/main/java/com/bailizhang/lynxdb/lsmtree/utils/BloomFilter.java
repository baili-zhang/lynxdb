package com.bailizhang.lynxdb.lsmtree.utils;

import com.bailizhang.lynxdb.core.utils.FileChannelUtils;
import com.bailizhang.lynxdb.core.utils.MethodUtils;

import java.lang.reflect.Method;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import static com.bailizhang.lynxdb.core.utils.PrimitiveTypeUtils.BYTE_BIT_COUNT;

/**
 * 28 个 hash 函数，bit 位应该是插入元素的 40 倍
 * 误判率约等于 3.37e-9
 */
public class BloomFilter {
    private static final int HASH_FUNC_SIZE = 28;
    private static final int BITS_TIMES = 40;

    private final int bitCount;
    private final byte[] data;

    public BloomFilter(int count) {
        this.bitCount = count * BITS_TIMES;

        int byteCount = bitCount / BYTE_BIT_COUNT;
        data = new byte[byteCount];
    }

    public BloomFilter(byte[] bits) {
        data = bits;
        bitCount = data.length * BYTE_BIT_COUNT;
    }

    public static BloomFilter from(Path filePath, int begin, int count) {
        FileChannel channel = FileChannelUtils.open(filePath, StandardOpenOption.READ);

        int length = (count * BITS_TIMES) / BYTE_BIT_COUNT;
        byte[] data = FileChannelUtils.read(channel, begin, length);

        return new BloomFilter(data);
    }

    public int byteCount() {
        return bitCount / BYTE_BIT_COUNT;
    }

    public boolean isExist(byte[] key) {
        for(int i = 1; i < HASH_FUNC_SIZE + 1; i ++) {
            int hash = hashCode(key, i);
            int remainder = hash % bitCount;

            // 第几个 byte
            int byteIndex = remainder / BYTE_BIT_COUNT;
            // byte 中的第几个 bit 位
            int bitIndex = remainder % BYTE_BIT_COUNT;

            byte current = data[byteIndex];
            if((current & ((byte) 0x01 << bitIndex)) == 0) {
                return false;
            }
        }

        return true;
    }

    public boolean isNotExist(byte[] key) {
        return !isExist(key);
    }

    public void setObj(byte[] key) {
        for(int i = 1; i < HASH_FUNC_SIZE + 1; i ++) {
            int hash = hashCode(key, i);
            int remainder = hash % bitCount;

            // 第几个 byte
            int byteIndex = remainder / BYTE_BIT_COUNT;
            // byte 中的第几个 bit 位
            int bitIndex = remainder % BYTE_BIT_COUNT;

            byte current = data[byteIndex];
            current |= (byte) 0x01 << bitIndex;
            data[byteIndex] = current;
        }
    }

    public byte[] data() {
        return data;
    }

    private static int hashCode(byte[] key, int n) {
        int hash = 0;
        for (byte b : key) {
            hash += b;
            hash += (hash << n + 3);
            hash ^= (hash >> n);
        }
        hash += (hash << 3);
        hash ^= (hash >> 11);
        hash += (hash << 15);
        return (hash & 0x7FFFFFFF);
    }
}
