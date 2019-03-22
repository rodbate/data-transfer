package com.github.rodbate.datatransfer.transport.netty.config;

import lombok.Getter;
import lombok.Setter;

/**
 * User: rodbate
 * Date: 2018/12/13
 * Time: 10:44
 */
@Getter
@Setter
public class NettyClientConfig extends NettyConfig {

    /**
     *  client worker group thread num
     */
    private int clientWorkerGroupThreadNum = 1;

    /**
     *  client executor thread num
     */
    private int clientExecutorThreadNum = 4;

    /**
     *  client connection timeout millis
     */
    private int clientConnectionTimeoutMillis = 5000;

}
