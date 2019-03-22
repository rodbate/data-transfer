package com.github.rodbate.datatransfer.transport.common;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * User: rodbate
 * Date: 2018/12/13
 * Time: 11:38
 */
public class NamedThreadFactory implements ThreadFactory {

    private final AtomicInteger idx = new AtomicInteger(1);
    private final String threadName;
    private final boolean daemon;

    public NamedThreadFactory(String threadName) {
        this.threadName = threadName;
        this.daemon = true;
    }

    public NamedThreadFactory(String threadName, boolean daemon) {
        this.threadName = threadName;
        this.daemon = daemon;
    }

    @Override
    public Thread newThread(Runnable r) {
        Thread t = new Thread(r);
        t.setName(threadName + "-" + idx.getAndIncrement());
        t.setDaemon(this.daemon);
        t.setPriority(Thread.NORM_PRIORITY);
        return t;
    }

}
