package com.bailizhang.lynxdb.lsmtree.utils;

import com.bailizhang.lynxdb.core.utils.FileChannelUtils;
import com.bailizhang.lynxdb.core.utils.MethodUtils;

import java.lang.reflect.Method;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;

import static com.bailizhang.lynxdb.core.utils.PrimitiveTypeUtils.BYTE_BIT_COUNT;

/**
 * 7 个 hash 函数，bit 位应该是插入元素的 10 倍
 * 误判率约等于 0.008
 */
public class BloomFilter {
    private static final int LIMIT_HASH_FUNC_SIZE = 7;
    private static final int TEN_TIMES = 10;

    private static final Method[] hashFunctions;

    private final int bitCount;
    private final byte[] data;

    static {
        try {
            Class<?> clazz = Class.forName("com.bailizhang.lynxdb.lsmtree.utils.HashFunctions");
            Method[] methods = clazz.getDeclaredMethods();

            hashFunctions = Arrays
                    .stream(methods)
                    .sorted((m1, m2) -> {
                        HashFunction anno1 = m1.getAnnotation(HashFunction.class);
                        HashFunction anno2 = m2.getAnnotation(HashFunction.class);
                        return Integer.compare(anno1.value(), anno2.value());
                    })
                    .limit(LIMIT_HASH_FUNC_SIZE)
                    .toArray(Method[]::new);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    public BloomFilter(int count) {
        this.bitCount = count * TEN_TIMES;

        int byteCount = bitCount / BYTE_BIT_COUNT;
        data = new byte[byteCount];
    }

    public BloomFilter(byte[] bits) {
        data = bits;
        bitCount = data.length * BYTE_BIT_COUNT;
    }

    public static BloomFilter from(Path filePath, int begin, int count) {
        FileChannel channel = FileChannelUtils.open(filePath, StandardOpenOption.READ);

        int length = (count * TEN_TIMES) / BYTE_BIT_COUNT;
        byte[] data = FileChannelUtils.read(channel, begin, length);

        return new BloomFilter(data);
    }

    public int byteCount() {
        return bitCount / BYTE_BIT_COUNT;
    }

    public boolean isExist(Object o) {
        for(Method hashFunction : hashFunctions) {
            int hash = (int) MethodUtils.invoke(hashFunction, this, o.toString());
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

    public boolean isNotExist(Object o) {
        return !isExist(o);
    }

    public void setObj(Object o) {
        for(Method hashFunction : hashFunctions) {
            int hash = (int) MethodUtils.invoke(hashFunction, this, o.toString());
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
}
