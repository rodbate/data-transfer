package com.github.rodbate.datatransfer.server.server;

import com.github.rodbate.datatransfer.server.metrics.MicrometerJmxMetricsConstant;
import com.github.rodbate.datatransfer.transport.common.NamedThreadFactory;
import com.google.common.hash.Hashing;
import com.google.common.primitives.Longs;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.jmx.JmxMeterRegistry;
import lombok.extern.slf4j.Slf4j;

import java.nio.charset.StandardCharsets;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * User: rodbate
 * Date: 2018/12/19
 * Time: 9:34
 */
@Slf4j
public class DeduplicateMessageService {

    private final Object lock = new Object();
    private final ScheduledExecutorService scheduledExecutorService;
    private DataContainer prevData = new DataContainer();
    private DataContainer currentData = new DataContainer();

    private final Counter duplicatedCounter;


    public DeduplicateMessageService(final JmxMeterRegistry jmxMeterRegistry) {
        this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("renew-message-hash-set-executor"));

        //metrics
        this.duplicatedCounter = Counter.builder(MicrometerJmxMetricsConstant.DEDUPLICATE_MESSAGE_SERVICE_DUPLICATED_COUNT_METRIC_NAME)
            .description("deduplicate message service duplicated count")
            .register(jmxMeterRegistry);
        Gauge.builder(MicrometerJmxMetricsConstant.DEDUPLICATE_MESSAGE_SERVICE_DATA_PREV_LOWER_EIGHT_SIZE_METRIC_NAME, this::getPreLowerEightDataSize)
            .description("deduplicate message service pre data lower eight size")
            .register(jmxMeterRegistry);
        Gauge.builder(MicrometerJmxMetricsConstant.DEDUPLICATE_MESSAGE_SERVICE_DATA_PREV_UPPER_EIGHT_SIZE_METRIC_NAME, this::getPreUpperEightDataSize)
            .description("deduplicate message service pre data upper eight size")
            .register(jmxMeterRegistry);
        Gauge.builder(MicrometerJmxMetricsConstant.DEDUPLICATE_MESSAGE_SERVICE_DATA_CURRENT_LOWER_EIGHT_SIZE_METRIC_NAME, this::getCurrentLowerEightDataSize)
            .description("deduplicate message service current data lower eight size")
            .register(jmxMeterRegistry);
        Gauge.builder(MicrometerJmxMetricsConstant.DEDUPLICATE_MESSAGE_SERVICE_DATA_CURRENT_UPPER_EIGHT_SIZE_METRIC_NAME, this::getCurrentUpperEightDataSize)
            .description("deduplicate message service current data upper eight size")
            .register(jmxMeterRegistry);
    }

    /**
     * start service
     */
    public void start() {
        this.scheduledExecutorService.scheduleAtFixedRate(this::renewMessageHashSet, 150, 150, TimeUnit.SECONDS);
    }

    /**
     * whether data is exist
     *
     * @param data data bytes
     * @return true if exists else false
     */
    public boolean isExistWithLock(final byte[] data) {
        boolean exist;
        synchronized (this.lock) {
            exist = this.currentData.isExist(data) || this.prevData.isExist(data);
        }
        if (exist) {
            if (log.isDebugEnabled()) {
                log.debug("duplicate message: {}", new String(data, StandardCharsets.UTF_8));
            }
            this.duplicatedCounter.increment();
        }
        return exist;
    }


    /**
     * whether data is exist
     *
     * @param data data bytes
     * @return true if exists else false
     */
    public boolean isExistWithoutLock(final byte[] data) {
        Objects.requireNonNull(data, "data");
        boolean exist = this.currentData.isExist(data) || this.prevData.isExist(data);
        if (exist) {
            if (log.isDebugEnabled()) {
                log.debug("duplicate message: {}", new String(data, StandardCharsets.UTF_8));
            }
            this.duplicatedCounter.increment();
        }
        return exist;
    }


    /**
     * add data to container
     *
     * @param data bytes data
     */
    public void addWithoutLock(final byte[] data) {
        Objects.requireNonNull(data, "data");
        this.currentData.add(data);
    }


    /**
     * add data to container if not exist
     *
     * @param data bytes data
     * @return true if not exist then add else false
     */
    public boolean addIfNotExistWithoutLock(final byte[] data) {
        Objects.requireNonNull(data, "data");
        boolean exist = isExistWithoutLock(data);
        if (!exist) {
            //add if not exist
            this.currentData.add(data);
        }
        return exist;
    }


    /**
     * shutdown service
     */
    public void shutdown(boolean shutdownNow) {
        if (shutdownNow) {
            this.scheduledExecutorService.shutdownNow();
        } else {
            this.scheduledExecutorService.shutdown();
            try {
                boolean terminated = this.scheduledExecutorService.awaitTermination(10, TimeUnit.SECONDS);
                if (!terminated) {
                    this.scheduledExecutorService.shutdownNow();
                }
            } catch (InterruptedException e) {
                this.scheduledExecutorService.shutdownNow();
            }
        }
    }


    private double getPreLowerEightDataSize() {
        synchronized (this.lock) {
            return this.prevData.lowerEight.size();
        }
    }

    private double getPreUpperEightDataSize() {
        synchronized (this.lock) {
            return this.prevData.upperEight.size();
        }
    }

    private double getCurrentLowerEightDataSize() {
        synchronized (this.lock) {
            return this.currentData.lowerEight.size();
        }
    }

    private double getCurrentUpperEightDataSize() {
        synchronized (this.lock) {
            return this.currentData.upperEight.size();
        }
    }

    private void renewMessageHashSet() {
        try {
            if (log.isDebugEnabled()) {
                log.debug("renewMessageHashSet task executing...");
            }
            synchronized (this.lock) {
                DataContainer oldPreData = this.prevData;
                oldPreData.clear();
                oldPreData = null;//help gc
                this.prevData = this.currentData;
                this.currentData = new DataContainer();

            }
        } catch (Throwable ex) {
            log.error("renewMessageHashSet exception", ex);
        }
    }


    public Object getLock() {
        return this.lock;
    }

    private static class DataContainer {
        private Set<Long> lowerEight = new LinkedHashSet<>();
        private Set<Long> upperEight = new LinkedHashSet<>();

        boolean isExist(final byte[] data) {
            byte[] bytes = Hashing.murmur3_128().hashBytes(data).asBytes();
            long lowerEightHash = Longs.fromBytes(bytes[7], bytes[6], bytes[5], bytes[4], bytes[3], bytes[2], bytes[1], bytes[0]);
            long upperEightHash = Longs.fromBytes(bytes[15], bytes[14], bytes[13], bytes[12], bytes[11], bytes[10], bytes[9], bytes[8]);
            return this.upperEight.contains(upperEightHash) && this.lowerEight.contains(lowerEightHash);
        }

        void add(final byte[] data) {
            byte[] bytes = Hashing.murmur3_128().hashBytes(data).asBytes();
            long lowerEightHash = Longs.fromBytes(bytes[7], bytes[6], bytes[5], bytes[4], bytes[3], bytes[2], bytes[1], bytes[0]);
            long upperEightHash = Longs.fromBytes(bytes[15], bytes[14], bytes[13], bytes[12], bytes[11], bytes[10], bytes[9], bytes[8]);
            this.upperEight.add(upperEightHash);
            this.lowerEight.add(lowerEightHash);
        }

        void clear() {
            this.lowerEight = null;
            this.upperEight = null;
        }
    }
}
