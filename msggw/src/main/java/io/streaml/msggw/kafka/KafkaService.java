package io.streaml.msggw.kafka;

/*
 * Copyright 2019 Streamlio, Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import com.google.common.util.concurrent.AbstractService;

import java.net.InetSocketAddress;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaService extends AbstractService {
    private static final Logger log = LoggerFactory.getLogger(KafkaService.class);
    private static final int MAX_FRAME_LENGTH = 100*1024*1024; // 100MB

    final EventLoopGroup bossGroup;
    final EventLoopGroup workerGroup;
    final ServerBootstrap bootstrap;
    final int port;
    final ScheduledExecutorService scheduler;
    final ConsumerGroups groups;

    ScheduledFuture<?> groupTimeoutFuture;

    public KafkaService(int port,
                        ScheduledExecutorService scheduler,
                        ConsumerGroups groups,
                        ChannelInboundHandlerAdapter handler) {
        bossGroup = new NioEventLoopGroup();
        workerGroup = new NioEventLoopGroup();
        bootstrap = new ServerBootstrap();
        bootstrap.group(bossGroup, workerGroup)
            .channel(NioServerSocketChannel.class)
            .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    public void initChannel(SocketChannel ch) throws Exception {
                        ch.pipeline().addLast(new LengthFieldPrepender(4));
                        ch.pipeline().addLast(new LengthFieldBasedFrameDecoder(MAX_FRAME_LENGTH, 0, 4, 0, 4));
                        ch.pipeline().addLast(new KafkaProtocolCodec());
                        ch.pipeline().addLast(handler);
                    }
                })
            .option(ChannelOption.SO_BACKLOG, 128)
            .childOption(ChannelOption.SO_KEEPALIVE, true);
        this.port = port;
        this.scheduler = scheduler;
        this.groups = groups;
    }

    @Override
    protected void doStart() {
        groupTimeoutFuture = scheduler.scheduleAtFixedRate(() -> {
                groups.processTimeouts();
            }, 1, 1, TimeUnit.SECONDS);

        ChannelFuture future = bootstrap.bind(port);
        future.addListener((f) -> {
                if (f.isSuccess()) {
                    notifyStarted();
                } else {
                    notifyFailed(f.cause());
                }
            });
    }

    @Override
    protected void doStop() {
        if (groupTimeoutFuture != null) {
            groupTimeoutFuture.cancel(true);
        }

        workerGroup.shutdownGracefully();
        bossGroup.shutdownGracefully();
        notifyStopped();
    }
}

