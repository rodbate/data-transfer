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
public class NettyServerConfig extends NettyConfig {

    /**
     * server listen host
     */
    private String host;

    /**
     * server listen port
     */
    private int listenPort = 9898;

    /**
     *  netty boss group thread num
     */
    private int serverBossGroupThreadNum = 1;

}
