package com.github.rodbate.datatransfer.common.utils;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;

/**
 * User: rodbate
 * Date: 2018/12/17
 * Time: 17:18
 */
public final class ClassUtil {

    private ClassUtil() {
    }

    /**
     * get default classloader
     *
     * @return default classloader
     */
    public static ClassLoader getDefaultClassLoader() {
        ClassLoader cl = null;
        try {
            cl = Thread.currentThread().getContextClassLoader();
        } catch (Throwable ignored) {
        }
        if (cl == null) {
            cl = ClassUtil.class.getClassLoader();
            if (cl == null) {
                try {
                    cl = ClassLoader.getSystemClassLoader();
                } catch (Throwable ignored) {
                }
            }
        }
        return cl;
    }

    /**
     * set field value
     *
     * @param target  object which field belong to
     * @param field  field
     * @param value  value
     */
    public static void setFieldValue(final Object target, final Field field, final Object value) {
        boolean accessible = field.isAccessible();
        try {
            if (!accessible) {
                field.setAccessible(true);
            }
            field.set(target, value);
        } catch (IllegalAccessException e) {
            throw new RuntimeException("fail to access field: " + target.getClass().getName() + "." + field.getName());
        } finally {
            field.setAccessible(accessible);
        }
    }


    /**
     * get declared fields
     *
     * @param clazz   class
     * @param fields  fields set
     * @param predicate test predicate
     */
    public static void getFieldsRecursively(final Class clazz, final Set<Field> fields, final Predicate<Field> predicate) {
        if (clazz == null || clazz == Object.class) {
            return;
        }
        Arrays.stream(Optional.ofNullable(clazz.getDeclaredFields()).orElse(new Field[0]))
            .filter(f -> predicate == null || predicate.test(f))
            .forEach(fields::add);
        getFieldsRecursively(clazz.getSuperclass(), fields, predicate);
    }

}
