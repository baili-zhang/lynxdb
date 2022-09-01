package com.bailizhang.lynxdb.core.lsm;

import com.bailizhang.lynxdb.core.enhance.EnhanceFile;

import java.io.IOException;
import java.lang.reflect.Method;

public class BloomFilter {
    private static final int LIMIT_HASH_FUNC_SIZE = 7;

    private final EnhanceFile file;
    private final int bitCount;
    private final Method[] hashFunctions = new Method[LIMIT_HASH_FUNC_SIZE];

    BloomFilter(EnhanceFile file, int bitCount) {
        this.file = file;
        this.bitCount = bitCount;

        if(file.length() < (long) bitCount >> 3) {
            throw new RuntimeException("Bloom filter is not init.");
        }

        try {
            Class<?> clazz = Class.forName("com.bailizhang.lynxdb.core.lsm.HashFunctions");
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

    private void setIndex(int index) throws IOException {
        int i = index >> 3;
        byte b = (byte) (1 << (index & 0x07));
        byte origin = file.readByte(i);
        origin |= b;
        file.writeByte(origin, i);
    }

    private boolean isIndexSet(int index) throws IOException {
        int i = index >> 3;
        byte b = (byte) (1 << (index & 0x07));
        byte origin = file.readByte(i);
        return (origin & b) != 0;
    }

    public boolean isKeyExist(String key) {
        try {
            for (int i = 0; i < LIMIT_HASH_FUNC_SIZE; i++) {
                int index = ((int) hashFunctions[i].invoke(this, key)) % bitCount;
                if(!isIndexSet(index)) {
                    return false;
                }
            }
            return true;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void setKey(String key) {
        try {
            for (int i = 0; i < LIMIT_HASH_FUNC_SIZE; i++) {
                int index = ((int) hashFunctions[i].invoke(this, key)) % bitCount;
                setIndex(index);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
