package com.github.rodbate.datatransfer.common;

import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.CountDownLatch;

/**
 * User: rodbate
 * Date: 2018/12/17
 * Time: 15:43
 */
@Slf4j
public final class ApplicationMain {

    private ApplicationMain() {
    }

    /**
     * start up application
     *
     * @param applicationName   application name
     * @param applicationBanner application banner
     * @param startUpHook execute before application start up
     * @param shutdownHook execute before application shutdown
     */
    public static void startUp(final String applicationName, final String applicationBanner, final Runnable startUpHook, final Runnable shutdownHook) {
        final CountDownLatch shutdown = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread("shutdown-hook-thread") {
            @Override
            public void run() {
                if (shutdownHook != null) {
                    shutdownHook.run();
                }
                shutdown.countDown();
            }
        });

        Thread t = new Thread("Application-Main-Thread") {
            @Override
            public void run() {
                log.info("{} start up now!", applicationName);
                if (applicationBanner != null && applicationBanner.trim().length() > 0) {
                    log.info(applicationBanner);
                }
                try {
                    shutdown.await();
                } catch (InterruptedException e) {
                    log.error(e.getMessage(), e);
                }
            }
        };
        t.setDaemon(false);

        if (startUpHook != null) {
            startUpHook.run();
        }
        t.start();
    }
}
