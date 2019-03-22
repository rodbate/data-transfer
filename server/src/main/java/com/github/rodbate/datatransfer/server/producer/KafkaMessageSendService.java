package com.github.rodbate.datatransfer.server.producer;

import com.github.rodbate.datatransfer.common.dto.KafkaMessageDto;
import com.github.rodbate.datatransfer.server.metrics.MicrometerJmxMetricsConstant;
import com.github.rodbate.datatransfer.server.config.ApplicationConfig;
import com.github.rodbate.datatransfer.server.server.DeduplicateMessageService;
import com.github.rodbate.datatransfer.transport.common.Constants;
import com.github.rodbate.datatransfer.transport.common.NamedThreadFactory;
import io.micrometer.core.instrument.Counter;
import io.micrometer.jmx.JmxMeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * User: rodbate
 * Date: 2018/12/17
 * Time: 16:58
 */
@Slf4j
public class KafkaMessageSendService {

    private final KafkaProducer<byte[], byte[]> producer;
    private final ExecutorService sendMessageExecutor;
    private final DeduplicateMessageService deduplicateMessageService;
    private final String topic;

    private final Counter sendBatchMessageSuccessTotalBytes;
    private final Counter sendBatchMessageSuccessTotalCount;
    private final Counter sendBatchMessageFailedTotalBytes;
    private final Counter sendBatchMessageFailedTotalCount;

    public KafkaMessageSendService(final ApplicationConfig config, final JmxMeterRegistry jmxMeterRegistry) {
        this.topic = config.getKafkaTopic();
        Properties props = new Properties();
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "DataTransferServerProducer");
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getKafkaBootstrapServers());
        props.put(ProducerConfig.ACKS_CONFIG, config.getKafkaAck());
        this.producer = new KafkaProducer<>(props, new ByteArraySerializer(), new ByteArraySerializer());

        final int sendMessageExecutorThreadNum = Math.max(16, Constants.CPU_CORE_NUM * 2);
        this.sendMessageExecutor = new ThreadPoolExecutor(
            sendMessageExecutorThreadNum,
            sendMessageExecutorThreadNum,
            1,
            TimeUnit.MINUTES,
            new LinkedBlockingQueue<>(),
            new NamedThreadFactory("send-message-executor")
        );
        this.deduplicateMessageService = new DeduplicateMessageService(jmxMeterRegistry);

        //metrics
        this.sendBatchMessageSuccessTotalBytes = Counter.builder(MicrometerJmxMetricsConstant.KAFKA_MESSAGE_SEND_BATCH_MESSAGE_SUCCESS_TOTAL_BYTES_METRIC_NAME)
            .description("kafka message send batch message success total bytes")
            .register(jmxMeterRegistry);
        this.sendBatchMessageSuccessTotalCount = Counter.builder(MicrometerJmxMetricsConstant.KAFKA_MESSAGE_SEND_BATCH_MESSAGE_SUCCESS_TOTAL_COUNT_METRIC_NAME)
            .description("kafka message send batch message success total count")
            .register(jmxMeterRegistry);
        this.sendBatchMessageFailedTotalBytes = Counter.builder(MicrometerJmxMetricsConstant.KAFKA_MESSAGE_SEND_BATCH_MESSAGE_FAILED_TOTAL_BYTES_METRIC_NAME)
            .description("kafka message send batch message failed total bytes")
            .register(jmxMeterRegistry);
        this.sendBatchMessageFailedTotalCount = Counter.builder(MicrometerJmxMetricsConstant.KAFKA_MESSAGE_SEND_BATCH_MESSAGE_FAILED_TOTAL_COUNT_METRIC_NAME)
            .description("kafka message send batch message failed total count")
            .register(jmxMeterRegistry);
    }

    /**
     * send message to kafka
     *
     * @param topic kafka topic
     * @param value message value
     * @return send future
     */
    public Future<RecordMetadata> sendMsg(final String topic, final byte[] value) {
        return this.producer.send(new ProducerRecord<>(topic, value));
    }


    /**
     * send batch message
     *
     * @param messageList message list
     * @return true or false
     */
    public boolean sendBatch(List<KafkaMessageDto> messageList) {
        if (messageList == null || messageList.size() == 0) {
            return true;
        }

        //filter message
        synchronized (this.deduplicateMessageService.getLock()) {
            messageList.removeIf(msg -> this.deduplicateMessageService.isExistWithoutLock(msg.getMsg()));

            if (messageList.size() > 0) {
                final CountDownLatch signal = new CountDownLatch(messageList.size());
                final AtomicBoolean hasFailed = new AtomicBoolean(false);

                final List<KafkaMessageDto> successMessages = new LinkedList<>();
                for (KafkaMessageDto msg : messageList) {
                    this.sendMessageExecutor.execute(() -> {
                        if (log.isDebugEnabled()) {
                            log.debug("send msg [{}] to kafka", msg.toString());
                        }
                        this.producer.send(new ProducerRecord<>(this.topic, msg.getMsg()), (metadata, exception) -> {
                            if (exception != null) {
                                //failed
                                hasFailed.set(true);
                                this.sendBatchMessageFailedTotalBytes.increment(msg.getMsg().length);
                                this.sendBatchMessageFailedTotalCount.increment();
                                log.error("send message ack exception", exception);
                            } else {
                                //success
                                successMessages.add(msg);
                                this.sendBatchMessageSuccessTotalBytes.increment(msg.getMsg().length);
                                this.sendBatchMessageSuccessTotalCount.increment();
                            }
                            signal.countDown();
                        });
                    });
                }

                boolean success = false;
                try {
                    success = signal.await(30, TimeUnit.SECONDS);
                    if (log.isDebugEnabled()) {
                        log.debug("send batch message countdown latch awaited for 5s, result is {}", success);
                    }
                } catch (InterruptedException e) {
                    log.error(e.getMessage(), e);
                }

                success = success && !hasFailed.get();

                //add success message
                successMessages.forEach(msg -> this.deduplicateMessageService.addWithoutLock(msg.getMsg()));

                return success;
            } else {
                return true;
            }
        }
    }

    /**
     * send batch message
     *
     * @param messageList message list
     * @return true or false
     */
    public boolean sendBatch0(List<KafkaMessageDto> messageList) {
        if (messageList == null || messageList.size() == 0) {
            return true;
        }

        //filter message
        synchronized (this.deduplicateMessageService.getLock()) {
            messageList.removeIf(kafkaMessageDto -> this.deduplicateMessageService.isExistWithoutLock(kafkaMessageDto.getMsg()));

            this.sendMessageExecutor.execute(() -> {
                for (KafkaMessageDto msg : messageList) {
                    if (log.isDebugEnabled()) {
                        log.debug("send msg [{}] to kafka", msg.toString());
                    }
                    this.producer.send(new ProducerRecord<>(this.topic, msg.getMsg()));
                }
            });

            messageList.forEach(msg -> this.deduplicateMessageService.addWithoutLock(msg.getMsg()));
        }
        return true;
    }

    /**
     * start service
     */
    public void start() {
        this.deduplicateMessageService.start();
    }


    /**
     * close producer
     */
    public void shutdown(boolean shutdownNow) {
        this.deduplicateMessageService.shutdown(shutdownNow);
        if (shutdownNow) {
            this.sendMessageExecutor.shutdownNow();
        } else {
            this.sendMessageExecutor.shutdown();
            try {
                boolean terminated = this.sendMessageExecutor.awaitTermination(15, TimeUnit.SECONDS);
                if (!terminated) {
                    this.sendMessageExecutor.shutdownNow();
                }
            } catch (InterruptedException e) {
                this.sendMessageExecutor.shutdownNow();
            }
            this.producer.close();
        }
    }
}
