package com.github.rodbate.datatransfer.client.config;

import lombok.Getter;
import lombok.Setter;

/**
 * User: rodbate
 * Date: 2018/12/17
 * Time: 17:00
 */
@Getter
@Setter
public class ApplicationConfig {
    private String logConfigFile;

    /**
     * kafka consumer config
     */
    private String kafkaBootstrapServers;
    private String kafkaConsumerGroup;
    private String kafkaSubscribeTopics;
    private int kafkaConsumerCount;

    private String remoteServerAddress;

}
