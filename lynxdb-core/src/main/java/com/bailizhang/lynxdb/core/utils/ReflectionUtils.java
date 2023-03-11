package com.bailizhang.lynxdb.core.utils;

import java.lang.reflect.InvocationTargetException;

public interface ReflectionUtils {
    static <T> T newObj(Class<T> clazz) {
        try {
            return clazz.getConstructor().newInstance();
        } catch (InstantiationException
                 | IllegalAccessException
                 | InvocationTargetException
                 | NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }
}
