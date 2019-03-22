package com.github.rodbate.datatransfer.server.server;

import com.github.rodbate.datatransfer.transport.netty.ChannelEvent;
import com.github.rodbate.datatransfer.transport.netty.ChannelEventListener;
import lombok.extern.slf4j.Slf4j;

/**
 * User: rodbate
 * Date: 2018/12/17
 * Time: 16:36
 */
@Slf4j
public class DefaultChannelEventListener implements ChannelEventListener {
    @Override
    public void onChannelEvent(ChannelEvent channelEvent) throws Exception {
        log.info("DefaultChannelEventListener - {}", channelEvent);
    }
}
