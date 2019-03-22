package com.github.rodbate.datatransfer.transport.netty;

import com.github.rodbate.datatransfer.transport.protocol.Packet;
import io.netty.channel.ChannelHandlerContext;

/**
 * User: rodbate
 * Date: 2018/12/13
 * Time: 19:45
 */
public interface NettyTransportRequestProcessor {

    /**
     * process transport request
     *
     * @param ctx     context
     * @param request request
     * @return response packet
     */
    Packet process(final ChannelHandlerContext ctx, final Packet request);
}
