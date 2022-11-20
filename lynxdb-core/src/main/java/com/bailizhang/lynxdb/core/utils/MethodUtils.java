package com.bailizhang.lynxdb.core.utils;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public interface MethodUtils {
    static Object invoke(Method method, Object obj, Object... args) {
        try {
            return method.invoke(obj, args);
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }
}
