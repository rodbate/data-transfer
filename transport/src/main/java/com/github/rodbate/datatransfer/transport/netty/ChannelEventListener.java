package com.github.rodbate.datatransfer.transport.netty;


import java.util.EventListener;

/**
 * User: rodbate
 * Date: 2018/12/13
 * Time: 18:13
 */
public interface ChannelEventListener extends EventListener {

    /**
     * invoked when channel event reached
     *
     * @param channelEvent channel event
     * @throws Exception ex
     */
    void onChannelEvent(final ChannelEvent channelEvent) throws Exception;
}
