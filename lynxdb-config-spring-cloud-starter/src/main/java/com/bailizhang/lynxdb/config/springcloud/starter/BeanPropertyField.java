package com.bailizhang.lynxdb.config.springcloud.starter;

import java.lang.reflect.Field;
import java.util.Objects;

public record BeanPropertyField(
        Object bean,
        Field field,
        String annotationValue
) {
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BeanPropertyField that = (BeanPropertyField) o;
        return Objects.equals(field, that.field);
    }

    @Override
    public int hashCode() {
        return Objects.hash(field);
    }
}
