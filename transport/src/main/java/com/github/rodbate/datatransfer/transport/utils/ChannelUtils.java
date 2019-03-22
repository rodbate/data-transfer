package com.github.rodbate.datatransfer.transport.utils;

import io.netty.channel.Channel;
import lombok.extern.slf4j.Slf4j;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Objects;

/**
 * User: rodbate
 * Date: 2018/12/13
 * Time: 14:43
 */
@Slf4j
public final class ChannelUtils {

    private ChannelUtils() {
        throw new IllegalStateException("No ChannelUtils Instance");
    }


    /**
     * close netty channel
     *
     * @param channel channel
     */
    public static void closeChannel(final Channel channel) {
        if (channel != null) {
            channel.close().addListener(future -> {
                log.info("close channel <{}>, result is {}", channel, future.isSuccess());
            });
        }
    }


    /**
     * get remote address string from channel
     *
     * @param channel channel
     * @return host:port
     */
    public static String getRemoteAddressString(final Channel channel) {
        if (channel != null) {
            InetSocketAddress socketAddress = (InetSocketAddress) channel.remoteAddress();
            return String.format("%s:%s", socketAddress.getHostString(), socketAddress.getPort());
        }
        return "";
    }


    /**
     * string address to socket address
     *
     * @param address address
     * @return SocketAddress
     */
    public static SocketAddress stringAddressToSocketAddress(String address) {
        Objects.requireNonNull(address, "address");
        String[] strings = address.split(":");
        return new InetSocketAddress(strings[0], Integer.valueOf(strings[1]));
    }

}
