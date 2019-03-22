package com.github.rodbate.datatransfer.common.utils;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.core.joran.spi.JoranException;
import org.slf4j.LoggerFactory;

/**
 * User: rodbate
 * Date: 2018/12/18
 * Time: 8:50
 */
public final class LogbackUtil {

    private LogbackUtil() {
    }

    /**
     * init logback configuration
     *
     * @param configFilePath config file path
     * @return logger context
     */
    public static LoggerContext initConfiguration(final String configFilePath) {
        LoggerContext context = (LoggerContext) LoggerFactory.getILoggerFactory();
        context.reset();
        JoranConfigurator configurator = new JoranConfigurator();
        configurator.setContext(context);
        try {
            configurator.doConfigure(ClassUtil.getDefaultClassLoader().getResourceAsStream(configFilePath));
            return context;
        } catch (JoranException e) {
            throw new RuntimeException("configure logback exception", e);
        }
    }

}
