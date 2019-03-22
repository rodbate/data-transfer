package com.github.rodbate.datatransfer.server.server;

import com.github.rodbate.datatransfer.common.constant.RequestCodeConstants;
import com.github.rodbate.datatransfer.server.config.ApplicationConfig;
import com.github.rodbate.datatransfer.server.producer.KafkaMessageSendService;
import com.github.rodbate.datatransfer.transport.netty.NettyTransportServer;
import com.github.rodbate.datatransfer.transport.netty.config.NettyServerConfig;
import io.micrometer.core.instrument.Clock;
import io.micrometer.jmx.JmxConfig;
import io.micrometer.jmx.JmxMeterRegistry;

/**
 * User: rodbate
 * Date: 2018/12/17
 * Time: 16:32
 */
public class ApplicationServer {

    private final NettyTransportServer server;
    private final KafkaMessageSendService kafkaMessageSendService;
    private final JmxMeterRegistry jmxMeterRegistry;

    public ApplicationServer(final ApplicationConfig config) {
        this.jmxMeterRegistry = new JmxMeterRegistry(JmxConfig.DEFAULT, Clock.SYSTEM);

        NettyServerConfig serverConfig = new NettyServerConfig();
        serverConfig.setListenPort(config.getServerPort());
        this.server = new NettyTransportServer(serverConfig);
        this.server.registerChannelEventListener(new DefaultChannelEventListener());
        this.kafkaMessageSendService = new KafkaMessageSendService(config, this.jmxMeterRegistry);
        this.server.registerProcessor(RequestCodeConstants.SEND_MQ_MESSAGE_CODE, new MqMessageRequestProcessor(this.kafkaMessageSendService, this.jmxMeterRegistry));
    }

    /**
     * start the server
     */
    public void start() {
        this.jmxMeterRegistry.start();
        this.server.start();
        this.kafkaMessageSendService.start();
    }

    /**
     * shutdown the server
     */
    public void shutdown() {
        this.jmxMeterRegistry.close();
        this.kafkaMessageSendService.shutdown(false);
        this.server.shutdown(false);
    }

}
