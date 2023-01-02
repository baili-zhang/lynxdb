package com.bailizhang.lynxdb.config.springcloud.starter;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.convert.ConversionService;
import org.springframework.core.convert.support.DefaultConversionService;
import org.springframework.core.env.MutablePropertySources;
import org.springframework.core.env.PropertySourcesPropertyResolver;
import org.springframework.lang.Nullable;
import org.springframework.util.ReflectionUtils;
import org.springframework.util.StringValueResolver;

import java.lang.reflect.Field;
import java.util.HashSet;

import static org.springframework.beans.factory.config.PlaceholderConfigurerSupport.*;

@Configuration
public class StringValuePostProcessor implements BeanPostProcessor {
    private final HashSet<BeanPropertyField> beanPropertyFields = new HashSet<>();

    public StringValuePostProcessor() {

    }

    @Nullable
    public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {
        Class<?> clazz = bean.getClass();
        Field[] fields = clazz.getDeclaredFields();

        for(Field field : fields) {
            Value annotation = field.getAnnotation(Value.class);
            if(annotation == null) {
                continue;
            }
            String value = annotation.value();
            beanPropertyFields.add(new BeanPropertyField(bean, field, value));
        }
        return bean;
    }

    public void reassign(MutablePropertySources propertySources) {
        PropertySourcesPropertyResolver propertyResolver = new PropertySourcesPropertyResolver(propertySources);
        propertyResolver.setPlaceholderPrefix(DEFAULT_PLACEHOLDER_PREFIX);
        propertyResolver.setPlaceholderSuffix(DEFAULT_PLACEHOLDER_SUFFIX);
        propertyResolver.setValueSeparator(DEFAULT_VALUE_SEPARATOR);

        StringValueResolver valueResolver = strVal -> {
            String resolved = propertyResolver.resolvePlaceholders(strVal).trim();
            return resolved.equals("") ? null : resolved;
        };

        for(BeanPropertyField beanPropertyField : beanPropertyFields) {
            String value = beanPropertyField.annotationValue();
            Object bean = beanPropertyField.bean();
            Field field = beanPropertyField.field();

            String resolvedValue = valueResolver.resolveStringValue(value);
            ConversionService conversionService = DefaultConversionService.getSharedInstance();
            if(conversionService.canConvert(String.class, field.getType())) {
                ReflectionUtils.makeAccessible(field);
                Object target = conversionService.convert(resolvedValue, field.getType());
                ReflectionUtils.setField(field, bean, target);
            } else {
                throw new ClassCastException();
            }
        }
    }
}
