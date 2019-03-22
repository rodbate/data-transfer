package com.github.rodbate.datatransfer.transport;

/**
 * User: rodbate
 * Date: 2018/12/13
 * Time: 10:36
 */
public interface TransportService {

    /**
     *  service start
     */
    void start();

    /**
     * service shutdown
     *
     * @param shutdownNow shut down now
     */
    void shutdown(final boolean shutdownNow);

}
