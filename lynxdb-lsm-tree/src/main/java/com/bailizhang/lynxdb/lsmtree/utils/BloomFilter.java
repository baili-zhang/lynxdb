package com.bailizhang.lynxdb.lsmtree.utils;

import com.bailizhang.lynxdb.core.utils.FileChannelUtils;
import com.bailizhang.lynxdb.core.utils.MethodUtils;

import java.lang.reflect.Method;
import java.nio.ByteBuffer;
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

    private final FileChannel fileChannel;
    private final int byteBegin;
    private final int byteCount;
    private final int bitCount;

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

    public BloomFilter(Path path, int byteBegin, int count) {
        this.fileChannel = FileChannelUtils.open(
                path,
                StandardOpenOption.WRITE,
                StandardOpenOption.READ
        );

        this.byteBegin = byteBegin;
        this.bitCount = count * TEN_TIMES;

        byteCount = bitCount / BYTE_BIT_COUNT;

        if(FileChannelUtils.sizeLessThan(fileChannel, byteBegin + byteCount)) {
            ByteBuffer buffer = ByteBuffer.allocate(byteCount);
            FileChannelUtils.write(fileChannel, buffer, byteBegin);
        }
    }

    public BloomFilter(Path path, int count) {
        this(path, 0, count);
    }

    public int byteCount() {
        return byteCount;
    }

    public boolean isExist(Object o) {
        for(Method hashFunction : hashFunctions) {
            int hash = (int) MethodUtils.invoke(hashFunction, this, o.toString());
            int remainder = hash % bitCount;

            // 第几个 byte
            int byteIndex = remainder / BYTE_BIT_COUNT;
            // byte 中的第几个 bit 位
            int bitIndex = remainder % BYTE_BIT_COUNT;

            byte current = FileChannelUtils.read(fileChannel, byteBegin + byteIndex);
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

            byte current = FileChannelUtils.read(fileChannel, byteBegin + byteIndex);
            current |= (byte) 0x01 << bitIndex;

            FileChannelUtils.write(fileChannel, current, byteIndex);
        }
    }
}
