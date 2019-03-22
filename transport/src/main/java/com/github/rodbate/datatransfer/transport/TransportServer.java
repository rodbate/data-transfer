package com.github.rodbate.datatransfer.transport;

import com.github.rodbate.datatransfer.transport.netty.response.ResponseFuture;
import com.github.rodbate.datatransfer.transport.exceptions.TransportSendException;
import com.github.rodbate.datatransfer.transport.exceptions.TransportTooMuchRequestException;
import com.github.rodbate.datatransfer.transport.protocol.Packet;
import io.netty.channel.Channel;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * User: rodbate
 * Date: 2018/12/13
 * Time: 10:42
 */
public interface TransportServer extends TransportService {

    /**
     * send sync request
     *
     * @param channel netty channel
     * @param packet request packet
     * @return response packet
     * @throws InterruptedException interrupted exception
     * @throws TransportSendException send exception
     * @throws TimeoutException request timeout exception
     */
    Packet sendSync(final Channel channel, final Packet packet) throws InterruptedException, TransportSendException, TimeoutException;


    /**
     * send sync request
     *
     * @param channel netty channel
     * @param packet request packet
     * @param timeout timeout duration
     * @param timeUnit timeout unit
     * @return response packet
     * @throws InterruptedException interrupted exception
     * @throws TransportSendException send exception
     * @throws TimeoutException request timeout exception
     */
    Packet sendSync(final Channel channel, final Packet packet, final long timeout, final TimeUnit timeUnit) throws InterruptedException, TransportSendException, TimeoutException;


    /**
     * send one way request
     *
     * @param channel netty channel
     * @param packet one way request channel
     */
    void sendOneWay(final Channel channel, final Packet packet);

    /**
     * send async request
     *
     * @param channel netty channel
     * @param packet request packet
     * @return ResponseFuture
     * @throws InterruptedException interrupted exception
     * @throws TransportSendException send exception
     * @throws TransportTooMuchRequestException too much request exception
     */
    ResponseFuture sendAsync(final Channel channel, final Packet packet)throws InterruptedException, TransportSendException, TransportTooMuchRequestException;

    /**
     * send async request
     *
     * @param channel netty channel
     * @param packet request packet
     * @param timeout request timeout duration
     * @param timeUnit request timeout unit
     * @return ResponseFuture
     * @throws InterruptedException interrupted exception
     * @throws TransportSendException send exception
     * @throws TransportTooMuchRequestException too much request exception
     */
    ResponseFuture sendAsync(final Channel channel, final Packet packet, final long timeout, final TimeUnit timeUnit)throws InterruptedException, TransportSendException, TransportTooMuchRequestException;
}
