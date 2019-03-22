package com.github.rodbate.datatransfer.transport;

import com.github.rodbate.datatransfer.transport.netty.response.ResponseFuture;
import com.github.rodbate.datatransfer.transport.exceptions.TransportConnectException;
import com.github.rodbate.datatransfer.transport.exceptions.TransportSendException;
import com.github.rodbate.datatransfer.transport.exceptions.TransportTooMuchRequestException;
import com.github.rodbate.datatransfer.transport.protocol.Packet;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * User: rodbate
 * Date: 2018/12/13
 * Time: 10:43
 */
public interface TransportClient extends TransportService {

    /**
     * send sync request
     *
     * @param address connect address
     * @param packet  request packet
     * @return response packet
     * @throws InterruptedException      interrupted exception
     * @throws TransportSendException    send exception
     * @throws TimeoutException          request timeout exception
     * @throws TransportConnectException connect exception
     */
    Packet sendSync(final String address, final Packet packet) throws InterruptedException, TransportSendException, TimeoutException, TransportConnectException;


    /**
     * send sync request
     *
     * @param address  connect address
     * @param packet   request packet
     * @param timeout  timeout duration
     * @param timeUnit timeout unit
     * @return response packet
     * @throws InterruptedException      interrupted exception
     * @throws TransportSendException    send exception
     * @throws TimeoutException          request timeout exception
     * @throws TransportConnectException connect exception
     */
    Packet sendSync(final String address, final Packet packet, final long timeout, final TimeUnit timeUnit) throws InterruptedException, TransportSendException, TimeoutException, TransportConnectException;


    /**
     * send one way request
     *
     * @param address netty channel
     * @param packet  one way request channel
     * @throws TransportConnectException transport connect exception
     */
    void sendOneWay(final String address, final Packet packet) throws TransportConnectException;

    /**
     * send async request
     *
     * @param address connect address
     * @param packet  request packet
     * @return ResponseFuture
     * @throws InterruptedException             interrupted exception
     * @throws TransportSendException           send exception
     * @throws TransportTooMuchRequestException too much request exception
     * @throws TransportConnectException        connect exception
     */
    ResponseFuture sendAsync(final String address, final Packet packet) throws InterruptedException, TransportSendException, TransportTooMuchRequestException, TransportConnectException;

    /**
     * send async request
     *
     * @param address  connect address
     * @param packet   request packet
     * @param timeout  request timeout duration
     * @param timeUnit request timeout unit
     * @return ResponseFuture
     * @throws InterruptedException             interrupted exception
     * @throws TransportSendException           send exception
     * @throws TransportTooMuchRequestException too much request exception
     * @throws TransportConnectException        connect exception
     */
    ResponseFuture sendAsync(final String address, final Packet packet, final long timeout, final TimeUnit timeUnit) throws InterruptedException, TransportSendException, TransportTooMuchRequestException, TransportConnectException;

}
