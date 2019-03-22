package com.github.rodbate.datatransfer.transport.netty.config;

import com.github.rodbate.datatransfer.transport.common.Constants;
import lombok.Getter;
import lombok.Setter;

/**
 * User: rodbate
 * Date: 2018/12/14
 * Time: 8:55
 */
@Getter
@Setter
public class NettyConfig {

    /**
     * worker group thread num
     */
    private int workerGroupThreadNum = Math.max(8, Constants.CPU_CORE_NUM * 2);

    /**
     * default event executor group thread num
     */
    private int defaultEventExecutorGroupThreadNum = Math.max(16, Constants.CPU_CORE_NUM * 2);

    /**
     * processor executor thread num
     */
    private int processorExecutorThreadNum = Math.max(32, Constants.CPU_CORE_NUM * 2);

    /**
     * response future listener executor thread num
     */
    private int responseFutureListenerExecutorThreadNum = Math.max(8, Constants.CPU_CORE_NUM * 2);

    /**
     * async request semaphore permits
     */
    private int asyncRequestSemaphorePermits = 512;

    /**
     * channel idle timeout seconds [default 2min]
     */
    private int channelIdleTimeoutSeconds = 120;

    /**
     *  tcp backlog
     */
    private int backlog = 1024;

    /**
     * send buffer size
     */
    private int sendBufferSize = 65535;

    /**
     * receive buffer size
     */
    private int receiveBufferSize = 65535;

    /**
     * ues epoll or not
     */
    private boolean useEpoll = true;
}
