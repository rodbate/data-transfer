package com.github.rodbate.datatransfer.transport.netty;

import com.github.rodbate.datatransfer.transport.netty.config.NettyClientConfig;
import com.github.rodbate.datatransfer.transport.netty.config.NettyServerConfig;
import com.github.rodbate.datatransfer.transport.netty.systemcode.NettyTransportSystemResponseCode;
import com.github.rodbate.datatransfer.transport.exceptions.TransportConnectException;
import com.github.rodbate.datatransfer.transport.exceptions.TransportSendException;
import com.github.rodbate.datatransfer.transport.exceptions.TransportTooMuchRequestException;
import com.github.rodbate.datatransfer.transport.protocol.Packet;
import lombok.extern.slf4j.Slf4j;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;


/**
 * User: rodbate
 * Date: 2018/12/14
 * Time: 17:46
 */
@Slf4j
public class NettyTransportTest extends NettyTransportBase {

    private NettyTransportServer server;
    private int serverListenPort;

    @Before
    public void setUp() {
        final NettyServerConfig config = new NettyServerConfig();
        this.serverListenPort = randomPort();
        config.setListenPort(this.serverListenPort);
        this.server = createServer(config);
        this.server.start();
    }

    @Test
    public void testSendSync_success() {
        int requestCode = 0;
        Packet requestPacket = Packet.createRequestPacket(requestCode);
        final String requestBody = "request packet body 1";
        final String responseBody = "response packet body 1";
        requestPacket.setBody(requestBody.getBytes(StandardCharsets.UTF_8));
        final StringBuilder receiveRequestBody = new StringBuilder();
        this.server.registerProcessor(requestCode, (ctx, request) -> {
            receiveRequestBody.append(new String(request.getBody(), StandardCharsets.UTF_8));
            Packet responsePacket = Packet.createSuccessResponsePacket();
            responsePacket.setBody(responseBody.getBytes(StandardCharsets.UTF_8));
            return responsePacket;
        });

        NettyTransportClient client = createClient(new NettyClientConfig());
        client.start();
        try {
            Packet response = client.sendSync("localhost:" + this.serverListenPort, requestPacket);
            log.info("response: " + response);
            Assert.assertTrue(response.isResponse());
            assertEquals(NettyTransportSystemResponseCode.SUCCESS, response.getCode());
            Assert.assertEquals(requestBody, receiveRequestBody.toString());
            Assert.assertEquals(responseBody, new String(response.getBody(), StandardCharsets.UTF_8));
        } catch (InterruptedException e) {
            log.error(e.getMessage(), e);
            Assert.fail("InterruptedException");
        } catch (TransportSendException e) {
            log.error(e.getMessage(), e);
            Assert.fail("TransportSendException");
        } catch (TimeoutException e) {
            log.error(e.getMessage(), e);
            Assert.fail("TimeoutException");
        } catch (TransportConnectException e) {
            log.error(e.getMessage(), e);
            Assert.fail("TransportConnectException");
        } finally {
            client.shutdown(true);
        }
    }

    @Test
    public void sendOneWay_success() {
        int requestCode = 0;
        Packet requestPacket = Packet.createRequestPacket(requestCode);
        final String requestBody = "request packet body 1";
        requestPacket.setBody(requestBody.getBytes(StandardCharsets.UTF_8));
        requestPacket.markOneWayRequest();
        final StringBuilder receiveRequestBody = new StringBuilder();
        final AtomicBoolean isOneWayRequest = new AtomicBoolean(false);
        final CountDownLatch signal = new CountDownLatch(1);
        this.server.registerProcessor(requestCode, (ctx, request) -> {
            isOneWayRequest.set(request.isOneWayRequest());
            receiveRequestBody.append(new String(request.getBody(), StandardCharsets.UTF_8));
            signal.countDown();
            return null;
        });

        NettyTransportClient client = createClient(new NettyClientConfig());
        client.start();
        try {
            client.sendOneWay("localhost:" + this.serverListenPort, requestPacket);
            signal.await();
            Assert.assertEquals(requestBody, receiveRequestBody.toString());
            Assert.assertTrue(isOneWayRequest.get());
        } catch (TransportConnectException e) {
            log.error(e.getMessage(), e);
            Assert.fail("TransportConnectException");
        } catch (InterruptedException e) {
            log.error(e.getMessage(), e);
            Assert.fail("InterruptedException");
        } finally {
            client.shutdown(true);
        }
    }

    @Test
    public void testSendAsync_success() {
        int requestCode = 0;
        Packet requestPacket = Packet.createRequestPacket(requestCode);
        final String requestBody = "request packet body 1";
        final String responseBody = "response packet body 1";
        requestPacket.setBody(requestBody.getBytes(StandardCharsets.UTF_8));
        final StringBuilder receiveRequestBody = new StringBuilder();
        this.server.registerProcessor(requestCode, (ctx, request) -> {
            receiveRequestBody.append(new String(request.getBody(), StandardCharsets.UTF_8));
            Packet responsePacket = Packet.createSuccessResponsePacket();
            responsePacket.setBody(responseBody.getBytes(StandardCharsets.UTF_8));
            return responsePacket;
        });

        NettyTransportClient client = createClient(new NettyClientConfig());
        client.start();
        try {
            final CountDownLatch signal = new CountDownLatch(1);
            client.sendAsync("localhost:" + this.serverListenPort, requestPacket).addListener(future -> {
                Assert.assertTrue(future.isSuccess());
                Packet response = null;
                try {
                    response = future.get();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                log.info("response: " + response);
                Assert.assertTrue(response.isResponse());
                assertEquals(NettyTransportSystemResponseCode.SUCCESS, response.getCode());
                Assert.assertEquals(requestBody, receiveRequestBody.toString());
                Assert.assertEquals(responseBody, new String(response.getBody(), StandardCharsets.UTF_8));
                signal.countDown();
            });

            //await
            Assert.assertTrue(signal.await(5, TimeUnit.SECONDS));
        } catch (InterruptedException e) {
            log.error(e.getMessage(), e);
            Assert.fail("InterruptedException");
        } catch (TransportSendException e) {
            log.error(e.getMessage(), e);
            Assert.fail("TransportSendException");
        } catch (TransportConnectException e) {
            log.error(e.getMessage(), e);
            Assert.fail("TransportConnectException");
        } catch (TransportTooMuchRequestException e) {
            log.error(e.getMessage(), e);
            Assert.fail("TransportTooMuchRequestException");
        } finally {
            client.shutdown(true);
        }
    }


    @Test
    public void testSendSync_connectFailed() {
        NettyTransportClient client = new NettyTransportClient(new NettyClientConfig(), null);
        client.start();

        Packet requestPacket = Packet.createRequestPacket(0);
        boolean connectFailed = false;
        try {
            client.sendSync("localhost:1234", requestPacket);
        } catch (InterruptedException e) {
            e.printStackTrace();
            Assert.fail("InterruptedException");
        } catch (TransportSendException e) {
            e.printStackTrace();
            Assert.fail("TransportSendException");
        } catch (TimeoutException e) {
            e.printStackTrace();
            Assert.fail("TimeoutException");
        } catch (TransportConnectException e) {
            e.printStackTrace();
            //reach here
            connectFailed = true;
        } finally {
            client.shutdown(true);
        }
        Assert.assertTrue(connectFailed);
    }


    @Test
    public void testSendSync_timeout() {
        int requestCode = 0;
        Packet requestPacket = Packet.createRequestPacket(requestCode);
        final String requestBody = "request packet body 1";
        requestPacket.setBody(requestBody.getBytes(StandardCharsets.UTF_8));
        this.server.registerProcessor(requestCode, (ctx, request) -> {
            try {
                Thread.sleep(3000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            return Packet.createSuccessResponsePacket();
        });

        NettyTransportClient client = createClient(new NettyClientConfig());
        client.start();
        boolean timeout = false;
        try {
            client.sendSync("localhost:" + this.serverListenPort, requestPacket, 1, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            log.error(e.getMessage(), e);
            Assert.fail("InterruptedException");
        } catch (TransportSendException e) {
            log.error(e.getMessage(), e);
            Assert.fail("TransportSendException");
        } catch (TimeoutException e) {
            //reach here
            timeout = true;
        } catch (TransportConnectException e) {
            log.error(e.getMessage(), e);
            Assert.fail("TransportConnectException");
        } finally {
            client.shutdown(true);
        }
        Assert.assertTrue(timeout);
    }


    @Test
    public void testSendAsync_connectFailed() {
        NettyTransportClient client = new NettyTransportClient(new NettyClientConfig(), null);
        client.start();

        Packet requestPacket = Packet.createRequestPacket(0);
        boolean connectFailed = false;
        try {
            client.sendAsync("localhost:1234", requestPacket);
        } catch (InterruptedException e) {
            e.printStackTrace();
            Assert.fail("InterruptedException");
        } catch (TransportSendException e) {
            e.printStackTrace();
            Assert.fail("TransportSendException");
        } catch (TransportTooMuchRequestException e) {
            e.printStackTrace();
            Assert.fail("TransportTooMuchRequestException");
        } catch (TransportConnectException e) {
            //reach here
            connectFailed = true;
        } finally {
            client.shutdown(true);
        }
        Assert.assertTrue(connectFailed);
    }

    @Test
    public void sendAsync_timeout() {
        this.server.registerProcessor(0, (ctx, request) -> {
            try {
                Thread.sleep(3000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            return Packet.createSuccessResponsePacket();
        });

        NettyTransportClient client = new NettyTransportClient(new NettyClientConfig(), null);
        client.start();

        Packet requestPacket = Packet.createRequestPacket(0);
        AtomicBoolean timeout = new AtomicBoolean(false);
        final CountDownLatch signal = new CountDownLatch(1);
        try {
            client.sendAsync("localhost:" + this.serverListenPort, requestPacket, 1, TimeUnit.SECONDS)
                .addListener(future -> {
                    timeout.set(future.isTimeout());
                    signal.countDown();
                });
        } catch (InterruptedException e) {
            e.printStackTrace();
            Assert.fail("InterruptedException");
        } catch (TransportSendException e) {
            e.printStackTrace();
            Assert.fail("TransportSendException");
        } catch (TransportTooMuchRequestException e) {
            e.printStackTrace();
            Assert.fail("TransportTooMuchRequestException");
        } catch (TransportConnectException e) {
            e.printStackTrace();
            Assert.fail("TransportConnectException");
        } finally {
            client.shutdown(true);
        }
        try {
            signal.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        Assert.assertTrue(timeout.get());
    }


    @Test
    public void testSendOneWay_connectFailed() {
        NettyTransportClient client = new NettyTransportClient(new NettyClientConfig(), null);
        client.start();

        Packet requestPacket = Packet.createRequestPacket(0);
        boolean connectFailed = false;
        try {
            client.sendOneWay("localhost:1234", requestPacket);
        } catch (TransportConnectException e) {
            //reach here
            connectFailed = true;
        } finally {
            client.shutdown(true);
        }
        Assert.assertTrue(connectFailed);
    }


    @After
    public void tearDown() {
        if (this.server != null) {
            this.server.shutdown(true);
        }
    }
}
