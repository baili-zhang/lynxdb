package com.bailizhang.lynxdb.core.utils;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

public interface FieldUtils {
    static boolean isAnnotated(Field field, Class<? extends Annotation> annotation) {
        Annotation anno = field.getAnnotation(annotation);
        return Objects.nonNull(anno);
    }

    static List<String> findNames(Collection<Field> fields) {
        return fields.stream().map(Field::getName).toList();
    }
}
