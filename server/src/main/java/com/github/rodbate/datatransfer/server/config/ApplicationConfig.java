package com.github.rodbate.datatransfer.server.config;

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
    private int serverPort;
    private String logConfigFile;

    /**
     * kafka producer config
     */
    private String kafkaBootstrapServers;
    private String kafkaTopic;
    private String kafkaAck;

}
