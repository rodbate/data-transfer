package com.github.rodbate.datatransfer.transport.netty;

import io.netty.channel.Channel;
import lombok.Getter;

import java.util.EventObject;

/**
 * User: rodbate
 * Date: 2018/12/13
 * Time: 18:08
 */
@Getter
public class ChannelEvent extends EventObject {

    private final ChannelEventType channelEventType;

    /**
     * @param channel netty channel
     * @param channelEventType channel event type
     * @throws IllegalArgumentException if source is null.
     */
    public ChannelEvent(Channel channel, ChannelEventType channelEventType) {
        super(channel);
        this.channelEventType = channelEventType;
    }

    @Override
    public String toString() {
        return "ChannelEvent{" +
            "channelEventType=" + channelEventType +
            "} " + super.toString();
    }
}
