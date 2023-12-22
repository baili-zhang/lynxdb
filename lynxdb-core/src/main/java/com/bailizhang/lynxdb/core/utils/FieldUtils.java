/*
 * Copyright 2022-2023 Baili Zhang.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
            Class<?> type = field.getType();

            try {
                field.set(obj, convert(value, type));
            } catch (IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        }
    }

    static Object convert(String value, Class<?> type) {
        if(type == String.class) {
            return value;
        } else if (type == Integer.class || type == int.class) {
            return Integer.parseInt(value);
        } else if (type == Long.class || type == long.class) {
            return Long.parseLong(value);
        } else {
            throw new RuntimeException();
        }
    }
}
