package com.github.rodbate.datatransfer.transport.exceptions;

/**
 * User: rodbate
 * Date: 2018/12/14
 * Time: 17:10
 */
public class TransportConnectException extends Exception {

    public TransportConnectException(String remoteAddress) {
        super(String.format("connect to remote address <%s> exception", remoteAddress));
    }

    public TransportConnectException(String remoteAddress, Throwable cause) {
        super(String.format("connect to remote address <%s> exception", remoteAddress), cause);
    }
}
