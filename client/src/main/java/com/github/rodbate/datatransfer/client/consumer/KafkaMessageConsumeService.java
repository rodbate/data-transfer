package com.github.rodbate.datatransfer.client.consumer;

import com.github.rodbate.datatransfer.client.config.ApplicationConfig;
import com.github.rodbate.datatransfer.common.constant.RequestCodeConstants;
import com.github.rodbate.datatransfer.common.dto.KafkaMessageDto;
import com.github.rodbate.datatransfer.transport.common.NamedThreadFactory;
import com.github.rodbate.datatransfer.transport.exceptions.TransportConnectException;
import com.github.rodbate.datatransfer.transport.exceptions.TransportSendException;
import com.github.rodbate.datatransfer.transport.netty.NettyTransportClient;
import com.github.rodbate.datatransfer.transport.netty.config.NettyClientConfig;
import com.github.rodbate.datatransfer.transport.protocol.Packet;
import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import io.micrometer.jmx.JmxConfig;
import io.micrometer.jmx.JmxMeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.xerial.snappy.Snappy;

import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * User: rodbate
 * Date: 2018/12/18
 * Time: 10:06
 */
@Slf4j
public class KafkaMessageConsumeService {

    private final AtomicBoolean started = new AtomicBoolean(false);
    private final AtomicBoolean shutdown = new AtomicBoolean(false);
    private final ExecutorService executorService;
    private final int consumerCount;
    private final Properties consumerProperties;
    private final Set<String> topics;
    private final String remoteServerAddress;
    //private final NettyTransportClient nettyTransportClient;

    private final JmxMeterRegistry jmxMeterRegistry;
    private final Timer sendMessageToRemoteServerTimer;
    private final Timer consumeMessageTimer;
    private final Timer kafkaCommitSyncTimer;
    private final Counter consumeMessageCounter;
    private final Counter resetOffsetCounter;
    private final Counter transportConnectExceptionCounter;
    private final Counter transportSendExceptionCounter;
    private final Counter transportTimeoutExceptionCounter;

    public KafkaMessageConsumeService(final ApplicationConfig config) {
        //metrics
        this.jmxMeterRegistry = new JmxMeterRegistry(JmxConfig.DEFAULT, Clock.SYSTEM);
        this.sendMessageToRemoteServerTimer = Timer.builder("KafkaMessageConsumeService.sendMessageToRemoteServerTimer")
            .register(this.jmxMeterRegistry);
        this.consumeMessageTimer = Timer.builder("KafkaMessageConsumeService.consumeMessageTimer")
            .register(this.jmxMeterRegistry);
        this.kafkaCommitSyncTimer = Timer.builder("KafkaMessageConsumeService.kafkaCommitSyncTimer")
            .register(this.jmxMeterRegistry);
        this.consumeMessageCounter = Counter.builder("KafkaMessageConsumeService.consumeMessageCounter")
            .register(this.jmxMeterRegistry);
        this.resetOffsetCounter = Counter.builder("KafkaMessageConsumeService.resetOffsetCounter")
            .register(this.jmxMeterRegistry);
        this.transportConnectExceptionCounter = Counter.builder("KafkaMessageConsumeService.transportConnectExceptionCounter")
            .register(this.jmxMeterRegistry);
        this.transportSendExceptionCounter = Counter.builder("KafkaMessageConsumeService.transportSendExceptionCounter")
            .register(this.jmxMeterRegistry);
        this.transportTimeoutExceptionCounter = Counter.builder("KafkaMessageConsumeService.transportTimeoutExceptionCounter")
            .register(this.jmxMeterRegistry);

        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getKafkaBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, config.getKafkaConsumerGroup());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 500);
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 15000);
        this.consumerProperties = props;
        this.consumerCount = config.getKafkaConsumerCount();
        this.topics = Arrays.stream(config.getKafkaSubscribeTopics().split(",")).map(String::trim).collect(Collectors.toSet());
        this.executorService = Executors.newFixedThreadPool(this.consumerCount, new NamedThreadFactory("kafka-consumer-executor"));
        this.remoteServerAddress = config.getRemoteServerAddress();
    }

    /**
     * start service
     */
    public void start() {
        if (this.started.compareAndSet(false, true)) {
            this.jmxMeterRegistry.start();
            //this.nettyTransportClient.start();
            for (int i = 0; i < this.consumerCount; i++) {
                this.executorService.execute(new ConsumerTask(i));
            }
        }
    }


    /**
     * shutdown
     */
    public void shutdown() {
        this.jmxMeterRegistry.close();
        shutdown(false);
    }


    /**
     * shutdown service
     *
     * @param shutdownNow shutdown now or not
     */
    public void shutdown(boolean shutdownNow) {
        if (this.started.get() && this.shutdown.compareAndSet(false, true)) {
            if (shutdownNow) {
                this.executorService.shutdownNow();
            } else {
                this.executorService.shutdown();
                try {
                    boolean terminated = this.executorService.awaitTermination(15, TimeUnit.SECONDS);
                    if (!terminated) {
                        this.executorService.shutdownNow();
                    }
                } catch (InterruptedException e) {
                    this.executorService.shutdownNow();
                }
            }
            this.started.set(false);
        }
    }

    private NettyTransportClient createClient(final String clientName) {
        return new NettyTransportClient(new NettyClientConfig(), clientName);
    }

    private class ConsumerTask implements Runnable {

        private final KafkaConsumer<byte[], byte[]> consumer;
        private final NettyTransportClient transportClient;

        private ConsumerTask(int num) {
            this.consumer = new KafkaConsumer<>(KafkaMessageConsumeService.this.consumerProperties);
            this.transportClient = createClient("consumer-" + num);
            this.transportClient.start();
        }

        @Override
        public void run() {
            this.consumer.subscribe(KafkaMessageConsumeService.this.topics);

            try {
                while (true) {
                    consumeMessageTimer.record(() -> {
                        try {
                            ConsumerRecords<byte[], byte[]> records = this.consumer.poll(Duration.ofMillis(100));
                            if (!records.isEmpty()) {
                                consumeMessageCounter.increment(records.count());
                            }

                            boolean rs = sendMessageToRemoteServerTimer.record(() -> sendMessageToRemoteServer(records));
                            if (rs) {
                                if (!records.isEmpty()) {
                                    //commit sync
                                    kafkaCommitSyncTimer.record((Runnable) this.consumer::commitSync);
                                }
                            } else {
                                //reset consumer offset
                                resetConsumerOffset(records).forEach(this.consumer::seek);
                                resetOffsetCounter.increment();
                            }
                        } catch (Throwable ex) {
                            log.error("consumer exception", ex);
                        }
                    });

                }
            } catch (Throwable e) {
                log.error(e.getMessage(), e);
            } finally {
                this.consumer.close();
                this.transportClient.shutdown(true);
            }
        }

        private boolean sendMessageToRemoteServer(final ConsumerRecords<byte[], byte[]> records) {
            List<KafkaMessageDto> packetBody = new LinkedList<>();
            if (records != null) {
                for (ConsumerRecord<byte[], byte[]> record : records) {
                    packetBody.add(new KafkaMessageDto(record.topic(), record.value()));
                }
            }

            boolean success = false;
            if (packetBody.size() > 0) {
                Packet request = Packet.createRequestPacket(RequestCodeConstants.SEND_MQ_MESSAGE_CODE);
                try {
                    request.setBody(Snappy.compress(KafkaMessageDto.encode(packetBody)));
                } catch (IOException e) {
                    throw new RuntimeException("snappy compress exception", e);
                }

                try {
                    Packet packet = this.transportClient
                        .sendSync(KafkaMessageConsumeService.this.remoteServerAddress, request, 30, TimeUnit.SECONDS);
                    success = packet.isSuccessResponse();
                } catch (InterruptedException e) {
                    log.error("InterruptedException", e);
                } catch (TransportSendException e) {
                    transportSendExceptionCounter.increment();
                    log.error("TransportSendException", e);
                } catch (TimeoutException e) {
                    transportTimeoutExceptionCounter.increment();
                    log.error("TimeoutException", e);
                } catch (TransportConnectException e) {
                    transportConnectExceptionCounter.increment();
                    log.error("TransportConnectException", e);
                } catch (Throwable e) {
                    log.error(e.getMessage(), e);
                }
            } else {
                success = true;
            }
            return success;
        }

        private Map<TopicPartition, Long /* offset */> resetConsumerOffset(final ConsumerRecords<byte[], byte[]> records) {
            if (records == null || records.isEmpty()) {
                return Collections.emptyMap();
            }
            final Map<TopicPartition, Long> offsets = new HashMap<>();
            for (ConsumerRecord<byte[], byte[]> record : records) {
                TopicPartition tp = new TopicPartition(record.topic(), record.partition());
                Long offset = offsets.get(tp);
                boolean override = false;
                if (offset == null || record.offset() < offset) {
                    offset = record.offset();
                    override = true;
                }
                if (override) {
                    offsets.put(tp, offset);
                }
            }
            if (log.isDebugEnabled()) {
                log.debug("reset consumer offset: {}", offsets.toString());
            }
            return offsets;
        }
    }


}
