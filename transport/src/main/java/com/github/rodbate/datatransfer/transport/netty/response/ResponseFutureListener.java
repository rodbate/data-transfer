package com.github.rodbate.datatransfer.transport.netty.response;

import java.util.EventListener;

/**
 * User: rodbate
 * Date: 2018/12/14
 * Time: 11:41
 */
public interface ResponseFutureListener extends EventListener {

    /**
     * operation complete invoked
     *
     * @param future response future
     */
    void operationComplete(final ResponseFuture future);
}
