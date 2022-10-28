package com.bailizhang.lynxdb.lsmtree.utils;

import java.lang.reflect.Method;

public class BloomFilter {
    private static final int LIMIT_HASH_FUNC_SIZE = 7;

    private static final Method[] hashFunctions = new Method[LIMIT_HASH_FUNC_SIZE];

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

    BloomFilter(int bitCount) {

    }

}
