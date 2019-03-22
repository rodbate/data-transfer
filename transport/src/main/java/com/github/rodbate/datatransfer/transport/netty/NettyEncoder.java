package com.github.rodbate.datatransfer.transport.netty;

import com.github.rodbate.datatransfer.transport.utils.ChannelUtils;
import com.github.rodbate.datatransfer.transport.protocol.Packet;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import lombok.extern.slf4j.Slf4j;

/**
 * User: rodbate
 * Date: 2018/12/13
 * Time: 11:13
 */
@Slf4j
public class NettyEncoder extends MessageToByteEncoder<Packet> {

    @Override
    protected void encode(ChannelHandlerContext ctx, Packet msg, ByteBuf out) throws Exception {
        try {
            //write header
            out.writeBytes(msg.encodeHeader());

            //write body
            if (msg.getBody() != null && msg.getBody().length > 0) {
                out.writeBytes(msg.getBody());
            }
        } catch (Exception e) {
            log.info("encode exception : ", e);
            if (msg != null) {
                log.info("encode packet : {}", msg);
            }
            ChannelUtils.closeChannel(ctx.channel());
        }
    }

}
