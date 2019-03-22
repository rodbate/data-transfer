package com.github.rodbate.datatransfer.transport.netty;

import com.github.rodbate.datatransfer.transport.utils.ChannelUtils;
import com.github.rodbate.datatransfer.transport.protocol.Packet;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import lombok.extern.slf4j.Slf4j;

/**
 * User: rodbate
 * Date: 2018/12/13
 * Time: 11:13
 */
@Slf4j
public class NettyDecoder extends LengthFieldBasedFrameDecoder {
    /**
     * max frame length 32MB
     */
    private static final int MAX_FRAME_LENGTH = 32 * 1024 * 1024;

    public NettyDecoder() {
        super(MAX_FRAME_LENGTH, 0, 4, 0, 4);
    }


    @Override
    protected Object decode(ChannelHandlerContext ctx, ByteBuf in) throws Exception {
        ByteBuf frame = null;
        try {
            frame = (ByteBuf) super.decode(ctx, in);

            if (frame == null) {
                return null;
            }

            return Packet.decode(frame.nioBuffer());
        } catch (Throwable ex) {
            log.error("NettyDecoder -> decode encounter exception", ex);
            ChannelUtils.closeChannel(ctx.channel());
        } finally {
            if (frame != null) {
                frame.release();
            }
        }
        return null;
    }

}
