package com.github.rodbate.datatransfer.transport.netty.response;

import com.github.rodbate.datatransfer.transport.exceptions.ResponseFutureFailedException;
import com.github.rodbate.datatransfer.transport.protocol.Packet;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;


/**
 * User: rodbate
 * Date: 2018/12/14
 * Time: 11:19
 */
@Slf4j
public class ResponseFuture {

    private final CountDownLatch signal = new CountDownLatch(1);
    private final long startTimeMillis = System.currentTimeMillis();
    private final AtomicBoolean semaphoreReleased = new AtomicBoolean(false);
    private final AtomicBoolean listenerInvoked = new AtomicBoolean(false);
    private final List<ResponseFutureListener> listeners = new CopyOnWriteArrayList<>();
    private final int seqId;
    private final long timeoutMillis;
    private final Semaphore semaphore;
    private final Executor executor;
    private volatile Packet response;
    private volatile Throwable cause;
    private volatile boolean sendRequestSuccess;


    public ResponseFuture(final int seqId, final long timeoutMillis, final Semaphore semaphore, final Executor executor) {
        this.seqId = seqId;
        if (timeoutMillis < 0) {
            throw new IllegalArgumentException("timeout millis >= 0");
        }
        this.timeoutMillis = timeoutMillis;
        this.semaphore = semaphore;
        this.executor = Objects.requireNonNull(executor, "executor");
    }

    /**
     * add response future listener
     *
     * @param listener listener
     * @return response future
     */
    public ResponseFuture addListener(ResponseFutureListener listener) {
        Objects.requireNonNull(listener, "listener");
        this.listeners.add(listener);
        return this;
    }


    /**
     * execute future listener
     */
    private void executeListeners() {
        if (this.listenerInvoked.compareAndSet(false, true)) {
            final List<ResponseFutureListener> listeners = this.listeners;
            for (ResponseFutureListener listener : listeners) {
                this.executor.execute(() -> {
                    try {
                        listener.operationComplete(this);
                    } catch (Throwable ex) {
                        log.error("invoke listener exception", ex);
                    }
                });
            }
        }
    }


    /**
     * set future response
     *
     * @param response response
     */
    public void setSuccessResponse(final Packet response) {
        this.response = Objects.requireNonNull(response, "response");
        this.signal.countDown();
        releaseSemaphore();
        executeListeners();
    }


    /**
     * set future failed cause
     *
     * @param cause cause
     */
    public void setFailedCause(final Throwable cause) {
        this.cause = Objects.requireNonNull(cause, "cause");
        this.signal.countDown();
        releaseSemaphore();
        executeListeners();
    }


    /**
     * get response
     *
     * @return response packet
     * @throws InterruptedException          interrupted
     * @throws ResponseFutureFailedException response future failed exception
     */
    public Packet get() throws InterruptedException, ResponseFutureFailedException {
        this.signal.await();
        if (this.cause != null) {
            throw new ResponseFutureFailedException(this.toString(), this.cause);
        }
        return this.response;
    }


    /**
     * get response with timeout
     *
     * @param timeout  timeout duration
     * @param timeUnit time unit
     * @return response packet
     * @throws InterruptedException          interrupted
     * @throws TimeoutException              timeout
     * @throws ResponseFutureFailedException response future failed exception
     */
    public Packet get(long timeout, TimeUnit timeUnit) throws InterruptedException, TimeoutException, ResponseFutureFailedException {
        boolean await = this.signal.await(timeout, timeUnit);
        if (await) {
            if (this.cause != null) {
                if (this.cause instanceof TimeoutException) {
                    throw (TimeoutException) this.cause;
                }
                throw new ResponseFutureFailedException(this.toString(), this.cause);
            }
            return this.response;
        } else {
            throw new TimeoutException(String.format("get response timeout, %dms", timeUnit.toMillis(timeout)));
        }
    }


    /**
     * release response semaphore
     */
    private void releaseSemaphore() {
        if (this.semaphore != null && this.semaphoreReleased.compareAndSet(false, true)) {
            this.semaphore.release();
        }
    }


    public boolean isSuccess() {
        return this.cause == null && this.response != null;
    }

    /**
     * whether the response is timeout
     *
     * @return timeout or not
     */
    public boolean isTimeout() {
        if (this.timeoutMillis == 0) {
            return false;
        }
        return System.currentTimeMillis() - this.startTimeMillis > this.timeoutMillis + 1000;
    }

    public int getSeqId() {
        return seqId;
    }


    public Throwable getCause() {
        return cause;
    }

    public boolean isSendRequestSuccess() {
        return sendRequestSuccess;
    }

    public void setSendRequestSuccess(boolean sendRequestSuccess) {
        this.sendRequestSuccess = sendRequestSuccess;
    }

    @Override
    public String toString() {
        return "ResponseFuture{" +
            "startTimeMillis=" + startTimeMillis +
            ", semaphoreReleased=" + semaphoreReleased +
            ", listenerInvoked=" + listenerInvoked +
            ", seqId=" + seqId +
            ", timeoutMillis=" + timeoutMillis +
            ", semaphore=" + semaphore +
            ", response=" + response +
            ", sendRequestSuccess=" + sendRequestSuccess +
            ", cause=" + cause +
            '}';
    }

}
