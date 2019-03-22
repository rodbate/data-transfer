package com.github.rodbate.datatransfer.transport.exceptions;

/**
 * User: rodbate
 * Date: 2018/12/14
 * Time: 15:21
 */
public class ResponseFutureFailedException extends Exception {

    public ResponseFutureFailedException(String responseFuture) {
        super(responseFuture);
    }

    public ResponseFutureFailedException(String responseFuture, Throwable cause) {
        super(responseFuture, cause);
    }
}
