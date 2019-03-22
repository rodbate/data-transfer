package com.github.rodbate.datatransfer.transport.exceptions;


import com.github.rodbate.datatransfer.transport.utils.ChannelUtils;
import io.netty.channel.Channel;


/**
 * User: rodbate
 * Date: 2018/12/14
 * Time: 14:53
 */
public class TransportSendException extends Exception {

    public TransportSendException(Channel channel) {
        super(String.format("send request to channel <{%s> failed", ChannelUtils.getRemoteAddressString(channel)));
    }

    public TransportSendException(Channel channel, Throwable cause) {
        super(String.format("send request to channel <{%s> failed", ChannelUtils.getRemoteAddressString(channel)), cause);
    }
}
