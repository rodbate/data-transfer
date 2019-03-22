package com.github.rodbate.datatransfer.common.config;

/**
 * User: rodbate
 * Date: 2018/12/17
 * Time: 15:39
 */
public interface ConfigLoader<T> {


    /**
     * load config properties
     *
     * @param t     config source
     * @param clazz config target class
     * @param <C>   config target
     * @return config
     */
    <C> C loadConfig(T t, Class<C> clazz);

}
