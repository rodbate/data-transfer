package com.github.rodbate.datatransfer.server;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.github.rodbate.datatransfer.common.ApplicationMain;
import com.github.rodbate.datatransfer.common.config.PropertiesConfigLoader;
import com.github.rodbate.datatransfer.common.constant.CommonConstants;
import com.github.rodbate.datatransfer.common.utils.LogbackUtil;
import com.github.rodbate.datatransfer.common.utils.PropertiesUtil;
import com.github.rodbate.datatransfer.server.config.ApplicationConfig;
import com.github.rodbate.datatransfer.server.server.ApplicationServer;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.util.Properties;

/**
 * User: rodbate
 * Date: 2018/12/17
 * Time: 14:57
 */
@Slf4j
public class DataTransferServerApplication {

    private static final String APPLICATION_PROPERTIES_FILE_PATH = "application.properties";

    /**
     * 主程序入口
     *
     * @param args args
     */
    public static void main(String[] args) {
        Properties properties = PropertiesUtil.load(APPLICATION_PROPERTIES_FILE_PATH, DataTransferServerApplication.class.getClassLoader());
        String logConfigFilePath = properties.getProperty(CommonConstants.LOG_CONFIG_FILE_KEY);
        if (StringUtils.isBlank(logConfigFilePath)) {
            throw new IllegalStateException(String.format("property %s require not null", CommonConstants.LOG_CONFIG_FILE_KEY));
        }
        LogbackUtil.initConfiguration(logConfigFilePath);
        ApplicationConfig config = new PropertiesConfigLoader().loadConfig(properties, ApplicationConfig.class);
        log.info("Application Config Values: \n {}", JSON.toJSONString(config, SerializerFeature.PrettyFormat));
        final ApplicationServer server = new ApplicationServer(config);
        ApplicationMain.startUp(DataTransferServerApplication.class.getSimpleName(), null, server::start, server::shutdown);
    }

}
