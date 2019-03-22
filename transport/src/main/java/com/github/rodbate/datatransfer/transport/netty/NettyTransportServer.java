package com.github.rodbate.datatransfer.transport.netty;

import com.github.rodbate.datatransfer.transport.common.NamedThreadFactory;
import com.github.rodbate.datatransfer.transport.netty.config.NettyServerConfig;
import com.github.rodbate.datatransfer.transport.netty.response.ResponseFuture;
import com.github.rodbate.datatransfer.transport.utils.ChannelUtils;
import com.github.rodbate.datatransfer.transport.TransportServer;
import com.github.rodbate.datatransfer.transport.common.Constants;
import com.github.rodbate.datatransfer.transport.exceptions.TransportSendException;
import com.github.rodbate.datatransfer.transport.exceptions.TransportTooMuchRequestException;
import com.github.rodbate.datatransfer.transport.protocol.Packet;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import org.apache.commons.lang3.StringUtils;

import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * User: rodbate
 * Date: 2018/12/13
 * Time: 11:15
 */
public class NettyTransportServer extends AbstractNettyTransport<NettyServerConfig> implements TransportServer {

    private final ServerBootstrap serverBootstrap;
    private final EventLoopGroup bossGroup;
    private final EventLoopGroup workerGroup;
    private final DefaultEventExecutorGroup defaultEventExecutorGroup;



    public NettyTransportServer(final NettyServerConfig nettyServerConfig) {
        super(nettyServerConfig, "server");
        this.serverBootstrap = new ServerBootstrap();

        //boss group
        int bossGroupSize = this.nettyConfig.getServerBossGroupThreadNum();
        if (bossGroupSize <= 0) {
            logWarningServerConfig("serverBossGroupThreadNum", bossGroupSize);
            bossGroupSize = 1;
        }
        if (useEpoll()) {
            this.bossGroup = new EpollEventLoopGroup(bossGroupSize);
        } else {
            this.bossGroup = new NioEventLoopGroup(bossGroupSize);
        }

        //worker group
        int workerGroupSize = this.nettyConfig.getWorkerGroupThreadNum();
        if (workerGroupSize <= 0) {
            logWarningServerConfig("serverWorkerGroupThreadNum", workerGroupSize);
            workerGroupSize = Math.max(8, Constants.CPU_CORE_NUM * 2);
        }
        if (useEpoll()) {
            this.workerGroup = new EpollEventLoopGroup(workerGroupSize);
        } else {
            this.workerGroup = new NioEventLoopGroup(workerGroupSize);
        }

        //default event executor group
        int defaultEventExecutorGroupThreadNum = this.nettyConfig.getDefaultEventExecutorGroupThreadNum();
        if (defaultEventExecutorGroupThreadNum <= 0) {
            logWarningServerConfig("serverExecutorThreadNum", defaultEventExecutorGroupThreadNum);
            defaultEventExecutorGroupThreadNum = Math.max(16, Constants.CPU_CORE_NUM * 2);
        }
        this.defaultEventExecutorGroup = new DefaultEventExecutorGroup(defaultEventExecutorGroupThreadNum, new NamedThreadFactory("server-handler-executor"));
    }


    @Override
    public void start() {
        if (this.running.compareAndSet(false, true)) {
            startChannelEventExecutor();

            ServerBootstrap serverBootstrap = this.serverBootstrap.group(this.bossGroup, this.workerGroup)
                .channel(useEpoll() ? EpollServerSocketChannel.class : NioServerSocketChannel.class)
                .option(ChannelOption.SO_BACKLOG, this.nettyConfig.getBacklog())
                .option(ChannelOption.SO_REUSEADDR, true)
                .option(ChannelOption.SO_RCVBUF, this.nettyConfig.getReceiveBufferSize())
                .childOption(ChannelOption.SO_SNDBUF, this.nettyConfig.getSendBufferSize())
                .childOption(ChannelOption.TCP_NODELAY, true)
                .childOption(ChannelOption.SO_KEEPALIVE, false)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) throws Exception {
                        ch.pipeline().addLast(
                            NettyTransportServer.this.defaultEventExecutorGroup,
                            new NettyEncoder(),
                            new NettyDecoder(),
                            new IdleStateHandler(0, 0, NettyTransportServer.this.nettyConfig.getChannelIdleTimeoutSeconds()),
                            new NettyServerManageHandler(),
                            new NettyServerHandler()
                        );
                    }
                });

            final String host = this.nettyConfig.getHost();
            final int port = this.nettyConfig.getListenPort();

            ChannelFuture startUpFuture;
            if (StringUtils.isNotBlank(host)) {
                startUpFuture = serverBootstrap.bind(new InetSocketAddress(host, port));
            } else {
                startUpFuture = serverBootstrap.bind(port);
            }

            try {
                startUpFuture.sync().channel();
                this.stopped.set(false);
                log.info(">>>>>>>>>>>>>>>>> server start up successfully, bind to [{}:{}]", StringUtils.isNoneBlank(host) ? host : "127.0.0.1", port);
            } catch (InterruptedException e) {
                throw new IllegalStateException(" server fail to start ", e);
            }
        }
    }

    @Override
    public void shutdown(final boolean shutdownNow) {
        if (this.stopped.compareAndSet(false, true)) {
            this.running.set(false);

            shutdownExecutor(shutdownNow);

            this.bossGroup.shutdownGracefully().addListener(future -> {
                log.info("shutdown server bossGroup, result is {}", future.isSuccess());
            });
            this.workerGroup.shutdownGracefully().addListener(future -> {
                log.info("shutdown server workerGroup, result is {}", future.isSuccess());
            });
            this.defaultEventExecutorGroup.shutdownGracefully().addListener(future -> {
                log.info("shutdown server defaultEventExecutorGroup, result is {}", future.isSuccess());
            });
        }
    }




    @Override
    public Packet sendSync(final Channel channel, final Packet packet) throws InterruptedException, TransportSendException, TimeoutException {
        return sendSyncInternal(channel, packet);
    }

    @Override
    public Packet sendSync(final Channel channel, final Packet packet, final long timeout, final TimeUnit timeUnit) throws InterruptedException, TransportSendException, TimeoutException {
        return sendSyncInternal(channel, packet, timeout, timeUnit);
    }

    @Override
    public void sendOneWay(final Channel channel, final Packet packet) {
        sendOneWayInternal(channel, packet);
    }

    @Override
    public ResponseFuture sendAsync(Channel channel, Packet packet) throws InterruptedException, TransportSendException, TransportTooMuchRequestException {
        return sendAsync(channel, packet, 0, TimeUnit.MILLISECONDS);
    }

    @Override
    public ResponseFuture sendAsync(final Channel channel, final Packet packet, final long timeout, final TimeUnit timeUnit) throws InterruptedException, TransportSendException, TransportTooMuchRequestException {
        return sendAsyncInternal(channel, packet, timeout, timeUnit);
    }


    private class NettyServerHandler extends SimpleChannelInboundHandler<Packet> {
        @Override
        protected void channelRead0(ChannelHandlerContext ctx, Packet msg) throws Exception {
            processTransportPacketFromNetty(ctx, msg);
        }
    }


    private class NettyServerManageHandler extends ChannelDuplexHandler {

        @Override
        public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
            log.info("{} channel <{}> registered", logPrefix(), ctx.channel());
            super.channelRegistered(ctx);
        }

        @Override
        public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
            log.info("{} channel <{}> unregistered", logPrefix(), ctx.channel());
            super.channelUnregistered(ctx);
        }

        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            log.info("{} channel <{}> active", logPrefix(), ctx.channel());
            offerChannelEvent(new ChannelEvent(ctx.channel(), ChannelEventType.CONNECT));
            super.channelActive(ctx);
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            log.info("{} channel <{}> inactive", logPrefix(), ctx.channel());
            offerChannelEvent(new ChannelEvent(ctx.channel(), ChannelEventType.CLOSE));
            super.channelInactive(ctx);
        }

        @Override
        public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
            if (evt instanceof IdleStateEvent) {
                IdleStateEvent event = (IdleStateEvent) evt;
                if (event.state() == IdleState.ALL_IDLE) {
                    log.error("{} channel <{}> encounter idle exception", logPrefix(), ctx.channel());
                    ChannelUtils.closeChannel(ctx.channel());
                    offerChannelEvent(new ChannelEvent(ctx.channel(), ChannelEventType.IDLE));
                }
            }
            super.userEventTriggered(ctx, evt);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            log.error(String.format("%s channel <%s> throw exception", logPrefix(), ctx.channel()), cause);
            ChannelUtils.closeChannel(ctx.channel());
            offerChannelEvent(new ChannelEvent(ctx.channel(), ChannelEventType.EXCEPTION));
        }

        private String logPrefix() {
            return "[Netty Server Pipeline] -";
        }
    }
}
