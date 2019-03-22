package com.github.rodbate.datatransfer.common.config;


import com.github.rodbate.datatransfer.common.utils.ClassUtil;
import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.Field;
import java.util.*;

/**
 * User: rodbate
 * Date: 2018/12/17
 * Time: 15:40
 */
@Slf4j
public class PropertiesConfigLoader implements ConfigLoader<Properties> {

    private final Set<String> delimiters = new HashSet<>();

    public PropertiesConfigLoader() {
        this(null);
    }

    public PropertiesConfigLoader(final List<String> delimiters) {
        addDefaultDelimiters();
        if (delimiters != null && delimiters.size() > 0) {
            this.delimiters.addAll(delimiters);
        }
    }

    private void addDefaultDelimiters() {
        this.delimiters.add("\\.");
        this.delimiters.add("-");
        this.delimiters.add("_");
    }

    @Override
    public <C> C loadConfig(Properties properties, Class<C> clazz) {
        C config;
        try {
            config = clazz.newInstance();
            final Set<Field> fields = new HashSet<>();
            ClassUtil.getFieldsRecursively(clazz, fields, this::hasSetterMethod);
            Map<String, String> fieldNameValues = toFieldNameValues(properties);
            fields.forEach(f -> {
                String name = f.getName();
                String value = fieldNameValues.remove(name);
                if (value == null) {
                    log.warn("not found config field value: {}", name);
                } else {
                    ClassUtil.setFieldValue(config, f, castFieldValueType(f, value));
                }
            });

            fieldNameValues.forEach((k, v) -> {
                log.warn("invalid config key value: [{}={}] for config: {}", k, v, config);
            });

            return config;
        } catch (InstantiationException e) {
            throw new RuntimeException("fail to instantiate class: " + clazz.getName(), e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException("fail to access class: " + clazz.getName(), e);
        }
    }

    private Object castFieldValueType(final Field field, String value) {
        Class<?> type = field.getType();
        Object v;
        if (type == boolean.class || type == Boolean.class) {
            v = Boolean.valueOf(value);
        } else if (type == byte.class || type == Byte.class) {
            v = Byte.valueOf(value);
        } else if (type == char.class || type == Character.class) {
            value = value.trim();
            if (value.length() > 1) {
                throw new RuntimeException(String.format("cannot cast config value to char, [fieldName=%s, value=%s]", field.getName(), value));
            }
            v = value.charAt(0);
        } else if (type == short.class || type == Short.class) {
            v = Short.valueOf(value);
        } else if (type == int.class || type == Integer.class) {
            v = Integer.valueOf(value);
        } else if (type == float.class || type == Float.class) {
            v = Float.valueOf(value);
        } else if (type == long.class || type == Long.class) {
            v = Long.valueOf(value);
        } else if (type == double.class || type == Double.class) {
            v = Double.valueOf(value);
        } else {
            //string
            v = value;
        }
        return v;
    }

    private Map<String, String> toFieldNameValues(final Properties properties) {
        Objects.requireNonNull(properties, "properties");
        Map<String, String> map = new HashMap<>();
        Set<String> propertyNames = properties.stringPropertyNames();
        if (propertyNames != null && propertyNames.size() > 0) {
            for (String name : propertyNames) {
                String value = properties.getProperty(name);
                map.put(toCamelName(name), value);
            }
        }
        return map;
    }

    private String toCamelName(String name) {
        final StringBuilder sb = new StringBuilder();
        for (String delimiter : this.delimiters) {
            String[] split = name.split(delimiter);
            if (split.length >= 2) {
                sb.append(split[0]);
                for (int i = 1; i < split.length; i++) {
                    sb.append(uppercaseFirstChar(split[i]));
                }
                return sb.toString();
            }
        }
        return name;
    }

    private boolean hasSetterMethod(final Field field) {
        Class<?> clazz = field.getDeclaringClass();
        String setterMethodName = "set" + uppercaseFirstChar(field.getName());
        boolean hasSetterMethod = false;
        try {
            clazz.getMethod(setterMethodName, field.getType());
            hasSetterMethod = true;
        } catch (NoSuchMethodException ignore) {
        }
        return hasSetterMethod;
    }

    private String uppercaseFirstChar(String str) {
        if (str == null || str.trim().length() == 0) {
            return str;
        }
        str = str.trim();
        return Character.toUpperCase(str.charAt(0)) + str.substring(1);
    }
}
