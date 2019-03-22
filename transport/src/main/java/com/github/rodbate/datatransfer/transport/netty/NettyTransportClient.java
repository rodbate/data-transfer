package com.github.rodbate.datatransfer.transport.netty;

import com.github.rodbate.datatransfer.transport.common.NamedThreadFactory;
import com.github.rodbate.datatransfer.transport.netty.config.NettyClientConfig;
import com.github.rodbate.datatransfer.transport.netty.response.ResponseFuture;
import com.github.rodbate.datatransfer.transport.utils.ChannelUtils;
import com.github.rodbate.datatransfer.transport.TransportClient;
import com.github.rodbate.datatransfer.transport.exceptions.TransportConnectException;
import com.github.rodbate.datatransfer.transport.exceptions.TransportSendException;
import com.github.rodbate.datatransfer.transport.exceptions.TransportTooMuchRequestException;
import com.github.rodbate.datatransfer.transport.protocol.Packet;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.net.SocketAddress;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * User: rodbate
 * Date: 2018/12/14
 * Time: 15:46
 */
@Slf4j
public class NettyTransportClient extends AbstractNettyTransport<NettyClientConfig> implements TransportClient {

    private final Bootstrap bootstrap;
    private final EventLoopGroup workerGroup;
    private final DefaultEventExecutorGroup defaultEventExecutorGroup;
    private final ConcurrentHashMap<String /* address */, ChannelWrapper> addressToChannelTable = new ConcurrentHashMap<>();
    private final Lock channelTableLock = new ReentrantLock();

    public NettyTransportClient(final NettyClientConfig nettyConfig, String clientName) {
        super(nettyConfig, StringUtils.isBlank(clientName) ? (clientName = "") : ((clientName =  clientName + "-") + "client"));
        this.bootstrap = new Bootstrap();

        //worker group
        int workerGroupSize = this.nettyConfig.getClientWorkerGroupThreadNum();
        if (workerGroupSize <= 0) {
            logWarningServerConfig(clientName + "clientWorkerGroupThreadNum", workerGroupSize);
            workerGroupSize = 4;
        }
        if (useEpoll()) {
            this.workerGroup = new EpollEventLoopGroup(workerGroupSize);
        } else {
            this.workerGroup = new NioEventLoopGroup(workerGroupSize);
        }

        //default event executor group
        int defaultEventExecutorGroupThreadNum = this.nettyConfig.getClientExecutorThreadNum();
        if (defaultEventExecutorGroupThreadNum <= 0) {
            logWarningServerConfig(clientName + "clientExecutorThreadNum", defaultEventExecutorGroupThreadNum);
            defaultEventExecutorGroupThreadNum = 4;
        }
        this.defaultEventExecutorGroup =
            new DefaultEventExecutorGroup(defaultEventExecutorGroupThreadNum, new NamedThreadFactory(clientName + "client-handler-executor"));
    }

    @Override
    public void start() {
        if (this.running.compareAndSet(false, true)) {
            startChannelEventExecutor();

            this.bootstrap.group(this.workerGroup)
                .channel(useEpoll() ? EpollSocketChannel.class : NioSocketChannel.class)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, this.nettyConfig.getClientConnectionTimeoutMillis())
                .option(ChannelOption.SO_RCVBUF, this.nettyConfig.getReceiveBufferSize())
                .option(ChannelOption.SO_SNDBUF, this.nettyConfig.getSendBufferSize())
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.SO_KEEPALIVE, false)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) throws Exception {
                        ch.pipeline().addLast(
                            defaultEventExecutorGroup,
                            new NettyEncoder(),
                            new NettyDecoder(),
                            new IdleStateHandler(0, 0, nettyConfig.getChannelIdleTimeoutSeconds()),
                            new NettyClientManageHandler(),
                            new NettyClientHandler()
                        );
                    }
                });
        }
    }

    @Override
    public void shutdown(final boolean shutdownNow) {
        if (this.stopped.compareAndSet(false, true)) {
            this.running.set(false);

            closeAllChannels();

            shutdownExecutor(shutdownNow);

            this.workerGroup.shutdownGracefully().addListener(future -> {
                log.info("shutdown client workerGroup, result is {}", future.isSuccess());
            });
            this.defaultEventExecutorGroup.shutdownGracefully().addListener(future -> {
                log.info("shutdown client defaultEventExecutorGroup, result is {}", future.isSuccess());
            });
        }
    }

    private void closeAllChannels() {
        this.addressToChannelTable.values().forEach(cw -> {
            if (cw.getChannel() != null) {
                ChannelUtils.closeChannel(cw.getChannel());
            }
        });
    }


    @Override
    public Packet sendSync(final String address, final Packet packet) throws InterruptedException, TransportSendException, TimeoutException, TransportConnectException {
        return sendSync(address, packet, 0, TimeUnit.MILLISECONDS);
    }

    @Override
    public Packet sendSync(String address, Packet packet, long timeout, TimeUnit timeUnit) throws InterruptedException, TransportSendException, TimeoutException, TransportConnectException {
        Channel channel = getOrCreateChannel(address);
        if (channel != null && channel.isActive()) {
            try {
                return sendSyncInternal(channel, packet, timeout, timeUnit);
            } catch (TransportSendException ex) {
                log.error(String.format("send sync request to remote address <%s> exception", address), ex);
                closeChannel(address, channel);
                throw ex;
            }
        } else {
            closeChannel(address, channel);
            log.error("send sync request connect exception, remote address <{}>", address);
            throw new TransportConnectException(address);
        }
    }

    @Override
    public void sendOneWay(final String address, final Packet packet) throws TransportConnectException {
        Channel channel = getOrCreateChannel(address);
        if (channel != null && channel.isActive()) {
            sendOneWayInternal(channel, packet);
        } else {
            closeChannel(address, channel);
            log.error("send sync request connect exception, remote address <{}>", address);
            throw new TransportConnectException(address);
        }
    }

    @Override
    public ResponseFuture sendAsync(String address, Packet packet) throws InterruptedException, TransportSendException, TransportTooMuchRequestException, TransportConnectException {
        return sendAsync(address, packet, 0, TimeUnit.MILLISECONDS);
    }

    @Override
    public ResponseFuture sendAsync(final String address, final Packet packet, final long timeout, final TimeUnit timeUnit) throws InterruptedException, TransportSendException, TransportTooMuchRequestException, TransportConnectException {
        Channel channel = getOrCreateChannel(address);
        if (channel != null && channel.isActive()) {
            try {
                return sendAsyncInternal(channel, packet, timeout, timeUnit);
            } catch (TransportSendException ex) {
                log.error(String.format("send async request to remote address <%s> exception", address), ex);
                closeChannel(address, channel);
                throw ex;
            }
        } else {
            closeChannel(address, channel);
            log.error("send async request connect exception, remote address <{}>", address);
            throw new TransportConnectException(address);
        }
    }


    private Channel getOrCreateChannel(final String address) {
        Objects.requireNonNull(address, "address");

        ChannelWrapper cw = this.addressToChannelTable.get(address);
        if (cw != null && cw.isConnected()) {
            return cw.getChannel();
        }

        try {
            if (this.channelTableLock.tryLock(3, TimeUnit.SECONDS)) {
                try {
                    boolean createNewChannel;
                    cw = this.addressToChannelTable.get(address);

                    if (cw != null) {
                        if (cw.isConnected()) {
                            return cw.getChannel();
                        } else if (!cw.channelFuture.isDone()) {
                            createNewChannel = false;
                        } else {
                            this.addressToChannelTable.remove(address);
                            createNewChannel = true;
                        }
                    } else {
                        createNewChannel = true;
                    }

                    if (createNewChannel) {
                        ChannelFuture future = this.bootstrap.connect(ChannelUtils.stringAddressToSocketAddress(address));
                        cw = new ChannelWrapper(future);
                        this.addressToChannelTable.put(address, cw);
                    }

                } catch (Throwable ex) {
                    log.error(ex.getMessage(), ex);
                } finally {
                    this.channelTableLock.unlock();
                }
            } else {
                log.error("try lock channelTableLock timeout");
            }

        } catch (InterruptedException ex) {
            log.error("try lock channelTableLock interrupted", ex);
        }

        if (cw != null) {
            boolean await = cw.channelFuture.awaitUninterruptibly(this.nettyConfig.getClientConnectionTimeoutMillis());
            if (await) {
                if (cw.isConnected()) {
                    return cw.getChannel();
                } else {
                    log.error("connect to remote address <{}> failed", address);
                }
            } else {
                log.error("connect to remote address <{}> timeout", address);
            }
        }
        return null;
    }


    private void closeChannel(final String address, final Channel channel) {
        if (StringUtils.isBlank(address) || channel == null) {
            return;
        }

        try {
            if (this.channelTableLock.tryLock(3, TimeUnit.SECONDS)) {
                try {
                    ChannelWrapper cw = this.addressToChannelTable.get(address);
                    log.info("close channel - begin to close channel <{}>, find {}", address, cw != null);

                    boolean remove;
                    if (cw == null) {
                        remove = false;
                        log.info("close channel - channel <{}>  has already removed", address);
                    } else if (cw.getChannel() != channel) {
                        remove = false;
                        log.info("close channel - channel <{}> removed before, new channel <{}> created, skip", channel, cw.getChannel());
                    } else {
                        remove = true;
                    }

                    if (remove) {
                        cw = this.addressToChannelTable.remove(address);
                        ChannelUtils.closeChannel(cw.getChannel());
                    }

                } catch (Throwable ex) {
                    log.error(ex.getMessage(), ex);
                } finally {
                    this.channelTableLock.unlock();
                }
            } else {
                log.error("close channel - try lock channelTableLock timeout");
            }

        } catch (InterruptedException ex) {
            log.error("close channel - try lock channelTableLock interrupted", ex);
        }
    }


    private class ChannelWrapper {
        final ChannelFuture channelFuture;

        private ChannelWrapper(ChannelFuture channelFuture) {
            this.channelFuture = Objects.requireNonNull(channelFuture, "channelFuture");
        }

        boolean isConnected() {
            return channelFuture.channel() != null && channelFuture.channel().isActive();
        }

        Channel getChannel() {
            return channelFuture.channel();
        }
    }

    private class NettyClientHandler extends SimpleChannelInboundHandler<Packet> {
        @Override
        protected void channelRead0(ChannelHandlerContext ctx, Packet msg) throws Exception {
            processTransportPacketFromNetty(ctx, msg);
        }
    }


    private class NettyClientManageHandler extends ChannelDuplexHandler {

        @Override
        public void connect(ChannelHandlerContext ctx, SocketAddress remoteAddress, SocketAddress localAddress, ChannelPromise promise) throws Exception {
            log.info("{} connect, local address <{}> -> remote address <{}>", logPrefix(), localAddress, remoteAddress);
            offerChannelEvent(new ChannelEvent(ctx.channel(), ChannelEventType.CONNECT));
            super.connect(ctx, remoteAddress, localAddress, promise);
        }

        @Override
        public void disconnect(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
            log.info("{} disconnect, remote channel address <{}>", logPrefix(), ChannelUtils.getRemoteAddressString(ctx.channel()));
            offerChannelEvent(new ChannelEvent(ctx.channel(), ChannelEventType.CLOSE));
            super.disconnect(ctx, promise);
        }

        @Override
        public void close(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
            log.info("{} close, remote channel address <{}>", logPrefix(), ChannelUtils.getRemoteAddressString(ctx.channel()));
            offerChannelEvent(new ChannelEvent(ctx.channel(), ChannelEventType.CLOSE));
            super.close(ctx, promise);
        }


        @Override
        public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
            if (evt instanceof IdleStateEvent) {
                IdleStateEvent event = (IdleStateEvent) evt;
                if (event.state() == IdleState.ALL_IDLE) {
                    log.error("{} channel <{}> encounter idle exception", logPrefix(), ctx.channel());
                    //closeChannel(ChannelUtils.getRemoteAddressString(ctx.channel()), ctx.channel());
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
            return "[Netty Client Pipeline] -";
        }
    }

}
