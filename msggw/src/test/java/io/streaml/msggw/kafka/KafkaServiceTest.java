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

import static org.apache.kafka.common.protocol.ApiKeys.API_VERSIONS;
import static org.apache.kafka.common.protocol.ApiKeys.METADATA;

import com.google.common.base.Ticker;
import com.google.common.collect.Lists;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import io.netty.util.ReferenceCountUtil;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.requests.ApiVersionsResponse;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.ProduceRequest;
import org.apache.kafka.common.requests.ProduceResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.junit.Test;

public class KafkaServiceTest {
    private static final Logger log = LoggerFactory.getLogger(KafkaServiceTest.class);

    @Test(timeout=10000)
    public void testStartHandleMessageStop() throws Exception {
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
        MockConsumerGroupStorage storage = new MockConsumerGroupStorage();

        int port = HandlerTestBase.selectPort();
        KafkaService service = new KafkaService(port, scheduler,
                new ConsumerGroupsImpl(new MockCHashGroup("self"),
                                       Ticker.systemTicker(), storage, scheduler),
                new GiveEmWhatTheyWantHandler());

        service.startAsync().awaitRunning();

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:" + port);
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<>(props);
        producer.send(new ProducerRecord<String, String>("my-topic", "key1", "value1"));
        producer.flush();

        service.stopAsync();
        scheduler.shutdownNow();
    }

    @Sharable
    class GiveEmWhatTheyWantHandler extends ChannelInboundHandlerAdapter {
        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) {
            log.info("Got message in dummy handler {}", msg);
            if (msg instanceof KafkaProtocolCodec.KafkaHeaderAndRequest) {
                KafkaProtocolCodec.KafkaHeaderAndRequest har = (KafkaProtocolCodec.KafkaHeaderAndRequest)msg;

                switch (har.getHeader().apiKey()) {
                case API_VERSIONS:
                    ctx.writeAndFlush(KafkaProtocolCodec.KafkaHeaderAndResponse.responseForRequest(
                                              har, ApiVersionsResponse.defaultApiVersionsResponse()));
                    break;
                case METADATA:
                    MetadataRequest mdReq = (MetadataRequest)har.getRequest();
                    InetSocketAddress nodeAddress = (InetSocketAddress)ctx.channel().localAddress();
                    Node node = new Node(0, nodeAddress.getHostString(), nodeAddress.getPort());
                    List<MetadataResponse.TopicMetadata> metadata = new ArrayList<>();
                    for (String topic : mdReq.topics()) {
                        metadata.add(new MetadataResponse.TopicMetadata(
                                     Errors.NONE, topic, false,
                                     Lists.newArrayList(
                                             new MetadataResponse.PartitionMetadata(Errors.NONE, 0, node,
                                                                                    Lists.newArrayList(node),
                                                                                    Lists.newArrayList(node),
                                                                                    Lists.newArrayList()))));
                    }
                    MetadataResponse response = new MetadataResponse(Lists.newArrayList(node),
                                                                     "blahblahcluster", 7000, metadata);

                    ctx.writeAndFlush(KafkaProtocolCodec.KafkaHeaderAndResponse.responseForRequest(har, response));
                    break;
                case PRODUCE:
                    ProduceRequest pReq = (ProduceRequest)har.getRequest();
                    Map<TopicPartition, ProduceResponse.PartitionResponse> resp = new HashMap<>();
                    for (Map.Entry<TopicPartition, MemoryRecords> e : pReq.partitionRecordsOrFail().entrySet()) {
                        log.info("Handling produce for partition {} - {}", e.getKey(), e.getValue());
                        resp.put(e.getKey(), new ProduceResponse.PartitionResponse(Errors.NONE));
                    }
                    ctx.writeAndFlush(KafkaProtocolCodec.KafkaHeaderAndResponse.responseForRequest(
                                              har, new ProduceResponse(resp)));
                    break;
                };
            }
            ReferenceCountUtil.release(msg);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            log.error("Got error in dummy handler", cause);
            ctx.close();
        }
    };
}
