package com.github.rodbate.datatransfer.server.server;

import com.github.rodbate.datatransfer.common.dto.KafkaMessageDto;
import com.github.rodbate.datatransfer.server.producer.KafkaMessageSendService;
import com.github.rodbate.datatransfer.transport.netty.NettyTransportRequestProcessor;
import com.github.rodbate.datatransfer.transport.netty.systemcode.NettyTransportSystemResponseCode;
import com.github.rodbate.datatransfer.transport.protocol.Packet;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import io.micrometer.jmx.JmxMeterRegistry;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;
import org.xerial.snappy.Snappy;

import java.io.IOException;
import java.util.List;

import static com.github.rodbate.datatransfer.server.metrics.MicrometerJmxMetricsConstant.*;

/**
 * User: rodbate
 * Date: 2018/12/17
 * Time: 16:38
 */
@Slf4j
public class MqMessageRequestProcessor implements NettyTransportRequestProcessor {

    private final KafkaMessageSendService kafkaMessageSendService;
    private final Timer sendBatchCostTimer;
    private final Counter sendBatchSuccessCounter;
    private final Counter sendBatchFailedCounter;
    private final Counter receiveBytesCounter;

    public MqMessageRequestProcessor(final KafkaMessageSendService kafkaMessageSendService, final JmxMeterRegistry jmxMeterRegistry) {
        this.kafkaMessageSendService = kafkaMessageSendService;

        //metrics
        this.sendBatchCostTimer = Timer.builder(KAFKA_MESSAGE_SEND_BATCH_COST_TIME_METRIC_NAME)
            .description("kafka message send batch cost time")
            .register(jmxMeterRegistry);
        this.sendBatchSuccessCounter = Counter.builder(KAFKA_MESSAGE_SEND_BATCH_INVOKED_SUCCESS_COUNT_METRIC_NAME)
            .description("kafka message send batch invoked success count")
            .register(jmxMeterRegistry);
        this.sendBatchFailedCounter = Counter.builder(KAFKA_MESSAGE_SEND_BATCH_INVOKED_FAILED_COUNT_METRIC_NAME)
            .description("kafka message send batch invoked failed count")
            .register(jmxMeterRegistry);
        this.receiveBytesCounter = Counter.builder("NettyReceiveBytesCounter")
            .description("netty receive bytes counter")
            .register(jmxMeterRegistry);
    }

    @Override
    public Packet process(ChannelHandlerContext ctx, Packet request) {
        this.receiveBytesCounter.increment(request.getPacketSize());
        List<KafkaMessageDto> messages;
        try {
            messages = KafkaMessageDto.decode(Snappy.uncompress(request.getBody()));
        } catch (IOException e) {
            throw new RuntimeException("snappy uncompress exception", e);
        }
        boolean success = this.sendBatchCostTimer.record(() -> this.kafkaMessageSendService.sendBatch(messages));
        if (success) {
            this.sendBatchSuccessCounter.increment();
            return Packet.createSuccessResponsePacket();
        } else {
            this.sendBatchFailedCounter.increment();
            return Packet.createResponsePacket(NettyTransportSystemResponseCode.SYSTEM_ERROR);
        }
    }


}
