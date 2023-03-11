package com.bailizhang.lynxdb.core.utils;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.Objects;

public interface FieldUtils {
    static boolean isAnnotated(Field field, Class<? extends Annotation> annotation) {
        Annotation anno = field.getAnnotation(annotation);
        return Objects.nonNull(anno);
    }

    static Object get(Object obj, Field field) {
        field.setAccessible(true);

        try {
            return field.get(obj);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    static void set(Object obj, String name, String value) {
        Class<?> clazz = obj.getClass();
        try {
            Field field = clazz.getDeclaredField(name);
            set(obj, field, value);
        } catch (NoSuchFieldException e) {
            throw new RuntimeException(e);
        }
    }

    static void set(Object obj, Field field, String value) {
        if(field.getType() == value.getClass()) {
            field.setAccessible(true);
            try {
                field.set(obj, value);
            } catch (IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
