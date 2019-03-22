package com.github.rodbate.datatransfer.common.config;

import java.util.Map;

/**
 * User: rodbate
 * Date: 2018/12/17
 * Time: 15:34
 */
public interface Config {

    /**
     * init config
     *
     * @param properties properties
     */
    void initConfig(final Map<String, ?> properties);

    /**
     * get property value
     *
     * @param key property key
     * @param <V> property value
     * @return property value
     */
    <V> V getProperty(final String key);
}
