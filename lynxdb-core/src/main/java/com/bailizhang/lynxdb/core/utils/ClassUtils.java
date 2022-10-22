package com.bailizhang.lynxdb.core.utils;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public interface ClassUtils {
    static boolean isByte(Class<?> clazz) {
        return Objects.equals(clazz, byte.class) || Objects.equals(clazz, Byte.class);
    }

    static boolean isInt(Class<?> clazz) {
        return Objects.equals(clazz, int.class) || Objects.equals(clazz, Integer.class);
    }

    static boolean isShort(Class<?> clazz) {
        return Objects.equals(clazz, short.class) || Objects.equals(clazz, Short.class);
    }

    static boolean isLong(Class<?> clazz) {
        return Objects.equals(clazz, long.class) || Objects.equals(clazz, Long.class);
    }

    static boolean isString(Class<?> clazz) {
        return Objects.equals(clazz, String.class);
    }

    static boolean isChar(Class<?> clazz) {
        return Objects.equals(clazz, char.class) || Objects.equals(clazz, Character.class);
    }

    static boolean isFloat(Class<?> clazz) {
        return Objects.equals(clazz, float.class) || Objects.equals(clazz, Float.class);
    }

    static boolean isDouble(Class<?> clazz) {
        return Objects.equals(clazz, double.class) || Objects.equals(clazz, Double.class);
    }

    static List<Field> findFields(Class<?> clazz, Class<? extends Annotation> annotation) {
        return Arrays.stream(clazz.getDeclaredFields())
                .filter(field -> FieldUtils.isAnnotated(field, annotation))
                .toList();
    }
}
