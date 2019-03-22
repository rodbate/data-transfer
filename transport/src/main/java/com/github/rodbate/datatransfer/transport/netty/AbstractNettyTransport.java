package com.github.rodbate.datatransfer.transport.netty;

import com.github.rodbate.datatransfer.transport.common.NamedThreadFactory;
import com.github.rodbate.datatransfer.transport.netty.config.NettyConfig;
import com.github.rodbate.datatransfer.transport.netty.response.ResponseFuture;
import com.github.rodbate.datatransfer.transport.netty.systemcode.NettyTransportSystemResponseCode;
import com.github.rodbate.datatransfer.transport.utils.ChannelUtils;
import com.github.rodbate.datatransfer.transport.common.Constants;
import com.github.rodbate.datatransfer.transport.exceptions.ResponseFutureFailedException;
import com.github.rodbate.datatransfer.transport.exceptions.TransportSendException;
import com.github.rodbate.datatransfer.transport.exceptions.TransportTooMuchRequestException;
import com.github.rodbate.datatransfer.transport.protocol.Packet;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.epoll.Epoll;
import org.apache.commons.lang3.SystemUtils;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * User: rodbate
 * Date: 2018/12/13
 * Time: 18:19
 */
public abstract class AbstractNettyTransport<C extends NettyConfig> {

    protected final Logger log = LoggerFactory.getLogger(getClass());
    protected final C nettyConfig;
    protected final ExecutorService processorExecutorService;
    private final ScheduledExecutorService scanResponseFutureService;
    private final ExecutorService channelEventExecutor;
    private final Semaphore asyncRequestSemaphore;
    private final Executor responseFutureListenerExecutor;
    private final List<ChannelEventListener> channelEventListeners = new CopyOnWriteArrayList<>();
    private final BlockingQueue<ChannelEvent> channelEventQueue = new LinkedBlockingQueue<>(100000);
    private final ConcurrentHashMap<Integer /* command code */, Pair<NettyTransportRequestProcessor, Executor>> requestProcessors =
        new ConcurrentHashMap<>(64);
    private final ConcurrentHashMap<Integer, ResponseFuture> seqIdToResponseFuture = new ConcurrentHashMap<>(512);
    protected AtomicBoolean running = new AtomicBoolean(false);
    protected AtomicBoolean stopped = new AtomicBoolean(true);
    private volatile NettyTransportRequestProcessor defaultRequestProcessor =
        (ctx, request) -> {
            Packet response = Packet.createResponsePacket(NettyTransportSystemResponseCode.REQUEST_CODE_NOT_SUPPORT);
            response.setRemark(String.format("request code[%d] not supported", request.getCode()));
            return response;
        };


    public AbstractNettyTransport(C nettyConfig, final String threadNamePrefix) {
        this.nettyConfig = Objects.requireNonNull(nettyConfig, "nettyConfig");
        this.channelEventExecutor = Executors.newSingleThreadExecutor(new NamedThreadFactory(threadNamePrefix + "-channel-event-listener-executor"));
        this.scanResponseFutureService = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory(threadNamePrefix + "-scan-response-future-executor"));

        int processorExecutorThreadNum = this.nettyConfig.getProcessorExecutorThreadNum();
        if (processorExecutorThreadNum <= 0) {
            logWarningServerConfig("processorExecutorThreadNum", processorExecutorThreadNum);
            processorExecutorThreadNum = Math.max(32, Constants.CPU_CORE_NUM * 2);
        }
        this.processorExecutorService = Executors.newFixedThreadPool(processorExecutorThreadNum, new NamedThreadFactory(threadNamePrefix + "-request-processor-executor"));

        int responseFutureListenerExecutorThreadNum = this.nettyConfig.getResponseFutureListenerExecutorThreadNum();
        if (responseFutureListenerExecutorThreadNum <= 0) {
            logWarningServerConfig("responseFutureListenerExecutorThreadNum", responseFutureListenerExecutorThreadNum);
            responseFutureListenerExecutorThreadNum = Math.max(8, Constants.CPU_CORE_NUM * 2);
        }
        this.responseFutureListenerExecutor = Executors.newFixedThreadPool(responseFutureListenerExecutorThreadNum, new NamedThreadFactory(threadNamePrefix + "-response-future-listener-executor"));

        int asyncRequestSemaphorePermits = this.nettyConfig.getAsyncRequestSemaphorePermits();
        if (asyncRequestSemaphorePermits <= 0) {
            logWarningServerConfig("asyncRequestSemaphorePermits", asyncRequestSemaphorePermits);
            asyncRequestSemaphorePermits = 512;
        }
        this.asyncRequestSemaphore = new Semaphore(asyncRequestSemaphorePermits);
    }


    /**
     * send sync request internal
     *
     * @param channel netty channel
     * @param request request packet
     * @return packet response
     * @throws InterruptedException   interrupted
     * @throws TimeoutException       timeout
     * @throws TransportSendException transport send ex
     */
    protected Packet sendSyncInternal(final Channel channel, final Packet request) throws InterruptedException, TimeoutException, TransportSendException {
        return sendSyncInternal(channel, request, 0, TimeUnit.MILLISECONDS);
    }


    /**
     * send sync request internal
     *
     * @param channel  netty channel
     * @param request  request packet
     * @param timeout  request timeout
     * @param timeUnit timeout unit
     * @return packet response
     * @throws TimeoutException       timeout
     * @throws InterruptedException   interrupted
     * @throws TransportSendException transport send ex
     */
    protected Packet sendSyncInternal(final Channel channel, final Packet request, final long timeout, final TimeUnit timeUnit) throws TimeoutException, InterruptedException, TransportSendException {
        final int seqId = request.getSeqId();

        try {
            ResponseFuture future = new ResponseFuture(seqId, timeUnit.toMillis(timeout), null, this.responseFutureListenerExecutor);
            this.seqIdToResponseFuture.put(seqId, future);

            channel.writeAndFlush(request).addListener(f -> {
                if (f.isSuccess()) {
                    future.setSendRequestSuccess(true);
                } else {
                    future.setSendRequestSuccess(false);
                    future.setFailedCause(f.cause());
                    this.seqIdToResponseFuture.remove(seqId);
                    log.error("failed to send request [{}] to channel <{}>", request, ChannelUtils.getRemoteAddressString(channel));
                }
            });
            if (timeout > 0) {
                return future.get(timeout, timeUnit);
            } else {
                return future.get();
            }
        } catch (ResponseFutureFailedException e) {
            throw new TransportSendException(channel, e);
        } finally {
            this.seqIdToResponseFuture.remove(seqId);
        }
    }


    /**
     * send one way request internal
     *
     * @param channel netty channel
     * @param request one way request packet
     */
    protected void sendOneWayInternal(final Channel channel, final Packet request) {
        if (!request.isOneWayRequest()) {
            throw new RuntimeException(String.format("request packet[%s] is not one way request", request));
        }
        channel.writeAndFlush(request);
    }

    /**
     * send async request
     *
     * @param channel netty channel
     * @param request request packet
     * @return response future
     * @throws InterruptedException interrupted exception
     * @throws TransportTooMuchRequestException too much exception
     * @throws TransportSendException send request exception
     */
    protected ResponseFuture sendAsyncInternal(final Channel channel, final Packet request) throws InterruptedException, TransportTooMuchRequestException, TransportSendException {
        return sendAsyncInternal(channel, request, 0, TimeUnit.MILLISECONDS);
    }

    /**
     * send async request
     *
     * @param channel netty channel
     * @param request request packet
     * @param timeout request timeout
     * @param timeUnit time unit
     * @return response future
     * @throws InterruptedException interrupted exception
     * @throws TransportTooMuchRequestException too much exception
     * @throws TransportSendException send request exception
     */
    protected ResponseFuture sendAsyncInternal(final Channel channel, final Packet request, final long timeout, final TimeUnit timeUnit) throws InterruptedException, TransportTooMuchRequestException, TransportSendException {
        final int seqId = request.getSeqId();
        boolean acquire = this.asyncRequestSemaphore.tryAcquire(timeout, timeUnit);
        if (acquire) {
            final ResponseFuture future = new ResponseFuture(seqId, timeUnit.toMillis(timeout), this.asyncRequestSemaphore, this.responseFutureListenerExecutor);
            this.seqIdToResponseFuture.put(seqId, future);
            try {
                channel.writeAndFlush(request).addListener(f -> {
                    if (f.isSuccess()) {
                        future.setSendRequestSuccess(true);
                    } else {
                        future.setSendRequestSuccess(false);
                        future.setFailedCause(f.cause());
                        this.seqIdToResponseFuture.remove(seqId);
                        log.error("failed to send request [{}] to channel <{}>", request, ChannelUtils.getRemoteAddressString(channel));
                    }
                });
                return future;
            } catch (Exception e) {
                future.setSendRequestSuccess(false);
                future.setFailedCause(e);
                this.seqIdToResponseFuture.remove(seqId);
                throw new TransportSendException(channel, e);
            }
        } else {
            String msg = String.format("cannot acquire semaphore[queueLen=%d, permits=%d] while send async request",
                this.asyncRequestSemaphore.getQueueLength(), this.asyncRequestSemaphore.availablePermits());
            log.error(msg);
            throw new TransportTooMuchRequestException();
        }
    }


    protected void startChannelEventExecutor() {
        this.channelEventExecutor.submit(this::handleChannelEventListener);
        this.scanResponseFutureService.scheduleAtFixedRate(this::scanResponseFutures, 500, 1000, TimeUnit.MILLISECONDS);
    }

    protected void logWarningServerConfig(final String key, final Object value) {
        log.warn("[WARNING] netty server config invalid setting: {}={}", key, value);
    }

    protected boolean useEpoll() {
        return this.nettyConfig.isUseEpoll() && SystemUtils.IS_OS_LINUX && Epoll.isAvailable();
    }

    protected void processTransportPacketFromNetty(final ChannelHandlerContext ctx, final Packet packet) {
        if (packet.isResponse()) {
            processResponse(ctx, packet);
        } else {
            processRequest(ctx, packet);
        }
    }


    private void processRequest(final ChannelHandlerContext ctx, final Packet packet) {
        final int code = packet.getCode();
        final int seqId = packet.getSeqId();
        NettyTransportRequestProcessor processor;
        Executor executor;
        Pair<NettyTransportRequestProcessor, Executor> pair = this.requestProcessors.get(code);
        if (pair == null) {
            processor = this.defaultRequestProcessor;
            executor = this.processorExecutorService;
        } else {
            processor = pair.getValue0();
            executor = pair.getValue1();
        }

        final Runnable task = () -> {
            try {
                Packet response = processor.process(ctx, packet);
                if (!packet.isOneWayRequest() && response != null) {
                    response.setSeqId(seqId);
                    response.markResponse();
                    ctx.writeAndFlush(response);
                }
            } catch (Throwable ex) {
                log.error("process request exception", ex);
                sendErrorResponse(ctx.channel(), NettyTransportSystemResponseCode.SYSTEM_ERROR, packet.getSeqId(), ex.getMessage());
            }
        };

        try {
            executor.execute(task);
        } catch (RejectedExecutionException ex) {
            log.error(ex.getMessage(), ex);
            sendErrorResponse(ctx.channel(), NettyTransportSystemResponseCode.SYSTEM_BUSY, seqId, ex.getMessage());
        } catch (Throwable ex) {
            log.error(ex.getMessage(), ex);
            sendErrorResponse(ctx.channel(), NettyTransportSystemResponseCode.SYSTEM_ERROR, seqId, ex.getMessage());
        }
    }


    private void processResponse(final ChannelHandlerContext ctx, final Packet packet) {
        final int seqId = packet.getSeqId();
        ResponseFuture responseFuture = this.seqIdToResponseFuture.remove(seqId);
        if (responseFuture != null) {
            responseFuture.setSuccessResponse(packet);
        } else {
            log.warn("cannot find matched response future of seqId: {}, remote channel <{}>", seqId, ChannelUtils.getRemoteAddressString(ctx.channel()));
        }
    }


    private void sendErrorResponse(final Channel channel, final int code, final int seqId, final String errorMsg) {
        Packet response = Packet.createResponsePacket(code);
        response.setSeqId(seqId);
        response.setRemark(errorMsg);
        channel.writeAndFlush(response);
    }


    /**
     * register default request processor
     *
     * @param processor request processor
     */
    public void registerDefaultProcessor(final NettyTransportRequestProcessor processor) {
        Objects.requireNonNull(processor, "NettyTransportRequestProcessor");
        this.defaultRequestProcessor = processor;
    }

    /**
     * register request processor
     *
     * @param code      request command code
     * @param processor request processor
     */
    public void registerProcessor(final int code, final NettyTransportRequestProcessor processor) {
        registerProcessor(code, processor, null);
    }

    /**
     * register request processor
     *
     * @param code        request command code
     * @param processor   request processor
     * @param executor    processor executor
     */
    public void registerProcessor(int code, NettyTransportRequestProcessor processor, Executor executor) {
        Objects.requireNonNull(processor, "NettyTransportRequestProcessor");
        if (executor == null) {
            executor = this.processorExecutorService;
        }
        this.requestProcessors.put(code, Pair.with(processor, executor));
    }


    protected void shutdownExecutor(final boolean shutdownNow) {
        if (shutdownNow) {
            this.channelEventExecutor.shutdownNow();
            this.scanResponseFutureService.shutdownNow();
        } else {
            this.channelEventExecutor.shutdown();

            try {
                boolean shutdown = this.channelEventExecutor.awaitTermination(15, TimeUnit.SECONDS);
                if (!shutdown) {
                    this.channelEventExecutor.shutdownNow();
                }
            } catch (InterruptedException e) {
                log.error("channelEventExecutor shutdown interrupted", e);
                this.channelEventExecutor.shutdownNow();
            }

            this.scanResponseFutureService.shutdown();
            try {
                boolean shutdown = this.scanResponseFutureService.awaitTermination(15, TimeUnit.SECONDS);
                if (!shutdown) {
                    this.scanResponseFutureService.shutdownNow();
                }
            } catch (InterruptedException e) {
                log.error("scanResponseFutureService shutdown interrupted", e);
                this.scanResponseFutureService.shutdownNow();
            }
        }
    }


    /**
     * register channel event listener
     *
     * @param listener listener
     */
    public void registerChannelEventListener(final ChannelEventListener listener) {
        Objects.requireNonNull(listener, "ChannelEventListener");
        this.channelEventListeners.add(listener);
    }

    /**
     * unregister channel event listener
     *
     * @param listener listener
     */
    public void unregisterChannelEventListener(final ChannelEventListener listener) {
        Objects.requireNonNull(listener, "ChannelEventListener");
        this.channelEventListeners.remove(listener);
    }

    /**
     * offer channel event to blocking queue
     *
     * @param event channel event
     */
    public void offerChannelEvent(final ChannelEvent event) {
        Objects.requireNonNull(event, "ChannelEvent");
        if (!this.channelEventQueue.offer(event)) {
            log.warn("channel event queue is full, queue size is {}", this.channelEventQueue.size());
        }
    }


    private void handleChannelEventListener() {
        while (this.running.get()) {
            ChannelEvent channelEvent = null;
            try {
                channelEvent = this.channelEventQueue.poll(30, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                log.error("channelEventQueue -> poll interrupted", e);
            }

            if (channelEvent != null) {
                //invoke listeners
                for (ChannelEventListener listener : this.channelEventListeners) {
                    try {
                        listener.onChannelEvent(channelEvent);
                    } catch (Exception ex) {
                        log.error(String.format("throw exception while invoking the listener[%s]", listener.getClass().getName()), ex);
                    }
                }
            }
        }
    }


    private void scanResponseFutures() {
        try {
            Iterator<Map.Entry<Integer, ResponseFuture>> iterator = this.seqIdToResponseFuture.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<Integer, ResponseFuture> entry = iterator.next();
                ResponseFuture future = entry.getValue();
                if (future.isTimeout()) {
                    iterator.remove();
                    future.setFailedCause(new TimeoutException("response future timeout"));
                    log.info("response future {} timeout", future);
                }
            }
        } catch (Throwable ex) {
            log.error("scan response future exception", ex);
        }
    }


}
