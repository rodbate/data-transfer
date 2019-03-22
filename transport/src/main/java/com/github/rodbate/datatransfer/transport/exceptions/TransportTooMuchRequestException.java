package com.github.rodbate.datatransfer.transport.exceptions;


/**
 * User: rodbate
 * Date: 2018/12/14
 * Time: 15:15
 */
public class TransportTooMuchRequestException extends Exception {

    public TransportTooMuchRequestException() {
        super("too much request exception");
    }

    public TransportTooMuchRequestException(Throwable cause) {
        super("too much request exception", cause);
    }
}
