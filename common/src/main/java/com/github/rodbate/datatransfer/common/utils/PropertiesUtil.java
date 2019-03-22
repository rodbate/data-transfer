package com.github.rodbate.datatransfer.common.utils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;
import java.util.Properties;

/**
 * User: rodbate
 * Date: 2018/12/17
 * Time: 17:13
 */
public final class PropertiesUtil {
    private PropertiesUtil() {
    }


    /**
     * load properties from file path
     *
     * @param filePath      properties file path
     * @param classLoader   classloader
     * @return properties
     */
    public static Properties load(final String filePath, ClassLoader classLoader) {
        Objects.requireNonNull(filePath, "filePath");
        if (classLoader == null) {
            classLoader = ClassUtil.getDefaultClassLoader();
        }
        InputStream inputStream = classLoader.getResourceAsStream(filePath);
        if (inputStream == null) {
            throw new RuntimeException("failed to load properties file: " + filePath);
        }
        Properties properties = new Properties();
        try {
            if (filePath.endsWith(".xml")) {
                properties.loadFromXML(inputStream);
            } else {
                properties.load(inputStream);
            }
        } catch (IOException e) {
            throw new RuntimeException("fail to load properties or xml : " + filePath, e);
        }
        return properties;
    }


}
