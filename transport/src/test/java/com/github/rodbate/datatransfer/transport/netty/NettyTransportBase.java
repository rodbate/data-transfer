package com.github.rodbate.datatransfer.transport.netty;

import com.github.rodbate.datatransfer.transport.BaseTest;
import com.github.rodbate.datatransfer.transport.netty.config.NettyClientConfig;
import com.github.rodbate.datatransfer.transport.netty.config.NettyServerConfig;

import java.net.ServerSocket;

/**
 * User: rodbate
 * Date: 2018/12/17
 * Time: 10:21
 */
public class NettyTransportBase extends BaseTest {


    NettyTransportServer createServer(final NettyServerConfig serverConfig) {
        return new NettyTransportServer(serverConfig);
    }

    NettyTransportClient createClient(final NettyClientConfig clientConfig) {
        return new NettyTransportClient(clientConfig, "");
    }


    int randomPort() {
        try(ServerSocket serverSocket = new ServerSocket(0)) {
            return serverSocket.getLocalPort();
        } catch (Throwable e){
            throw new RuntimeException("find unused port exception", e);
        }
    }
}
