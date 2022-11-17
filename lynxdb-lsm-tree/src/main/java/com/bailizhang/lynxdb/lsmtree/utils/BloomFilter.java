package com.bailizhang.lynxdb.lsmtree.utils;

import com.bailizhang.lynxdb.core.utils.MethodUtils;

import java.lang.reflect.Method;
import java.nio.channels.FileChannel;

import static com.bailizhang.lynxdb.core.utils.PrimitiveTypeUtils.BYTE_BIT_COUNT;

/**
 * 7 个 hash 函数，bit 位应该是插入元素的 10 倍
 * 误判率约等于 0.008
 */
public class BloomFilter {
    private static final int LIMIT_HASH_FUNC_SIZE = 7;

    private static final Method[] hashFunctions = new Method[LIMIT_HASH_FUNC_SIZE];

    private final FileChannel fileChannel;
    private final int byteBegin;
    private final int bitCount;

    static {
        try {
            Class<?> clazz = Class.forName("com.bailizhang.lynxdb.lsmtree.utils.HashFunctions");
            Method[] methods = clazz.getDeclaredMethods();
            for (Method method : methods) {
                HashFunction annotation = method.getAnnotation(HashFunction.class);
                int i = annotation.value();
                if(i < LIMIT_HASH_FUNC_SIZE) {
                    hashFunctions[i] = method;
                }
            }
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    public BloomFilter(FileChannel fileChannel, int byteBegin, int bitCount) {
        this.fileChannel = fileChannel;
        this.byteBegin = byteBegin;
        this.bitCount = bitCount;
    }

    public boolean isExist(Object o) {
        return false;
    }

    public void setObj(Object o) {
        for(Method hashFunction : hashFunctions) {
            int hash = (int) MethodUtils.invoke(hashFunction, o.toString());
            int remainder = hash % bitCount;

            // 第几个 byte
            int byteIndex = remainder / BYTE_BIT_COUNT;
            // byte 中的第几个 bit 位
            int bitIndex = remainder % BYTE_BIT_COUNT;

        }
    }
}
