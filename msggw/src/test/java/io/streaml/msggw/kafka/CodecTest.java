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

import static org.mockito.Mockito.mock;
import static org.apache.kafka.common.protocol.CommonFields.THROTTLE_TIME_MS;

import com.google.common.collect.Lists;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.ReferenceCountUtil;

import io.streaml.msggw.kafka.KafkaProtocolCodec.KafkaHeaderAndRequest;
import io.streaml.msggw.kafka.KafkaProtocolCodec.KafkaHeaderAndResponse;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ApiVersionsRequest;
import org.apache.kafka.common.requests.ApiVersionsResponse;
import org.apache.kafka.common.requests.JoinGroupRequest;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.ResponseHeader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.junit.Assert;
import org.junit.Test;

public class CodecTest {
    private final static Logger log = LoggerFactory.getLogger(CodecTest.class);

    @Test
    public void testEncode() throws Exception {
        KafkaProtocolCodec codec = new KafkaProtocolCodec();

        KafkaHeaderAndRequest request = new KafkaHeaderAndRequest(
                new RequestHeader(ApiKeys.API_VERSIONS, ApiKeys.API_VERSIONS.latestVersion(), "foobar", 1234),
                new ApiVersionsRequest.Builder().build(),
                Unpooled.buffer(10));
        KafkaHeaderAndResponse response = KafkaHeaderAndResponse.responseForRequest(
                request, ApiVersionsResponse.defaultApiVersionsResponse());

        List<Object> out = new ArrayList<Object>();
        codec.encode(mock(ChannelHandlerContext.class), response, out);

        Assert.assertEquals(out.size(), 1);
        Assert.assertTrue(out.get(0) instanceof ByteBuf);

        ByteBuf bb = (ByteBuf)out.get(0);
        ByteBuffer nio = bb.nioBuffer();
        ResponseHeader header = ResponseHeader.parse(nio);
        Assert.assertEquals(header.correlationId(), 1234);
        ApiVersionsResponse parsedResponse = ApiVersionsResponse.parse(
                nio, response.getApiVersion());
        Assert.assertEquals(parsedResponse.error(), Errors.NONE);
        Assert.assertEquals(parsedResponse.throttleTimeMs(), THROTTLE_TIME_MS.defaultValue);
    }

    @Test
    public void testDecode() throws Exception {
        KafkaProtocolCodec codec = new KafkaProtocolCodec();

        ApiVersionsRequest request = (new ApiVersionsRequest.Builder()).build();
        RequestHeader header = new RequestHeader(ApiKeys.API_VERSIONS, request.version(),
                                                 "foobar", 1234);
        ByteBuffer serialized = request.serialize(header);
        int size = serialized.remaining();
        ByteBuf toDecode = Unpooled.buffer(size);
        toDecode.writeBytes(serialized);

        List<Object> out = new ArrayList<Object>();
        codec.decode(mock(ChannelHandlerContext.class), toDecode, out);

        Assert.assertEquals(out.size(), 1);
        Assert.assertTrue(out.get(0) instanceof KafkaHeaderAndRequest);

        KafkaHeaderAndRequest har = (KafkaHeaderAndRequest)out.get(0);
        Assert.assertEquals(har.getHeader().apiKey(), ApiKeys.API_VERSIONS);
        Assert.assertEquals(har.getHeader().apiVersion(), request.version());
        Assert.assertEquals(har.getHeader().clientId(), "foobar");
        Assert.assertEquals(har.getHeader().correlationId(), 1234);
        Assert.assertTrue(har.getRequest() instanceof ApiVersionsRequest);
    }

    @Test
    public void testDecodeByteBuffers() throws Exception {
        final int numBytes = 100;
        KafkaProtocolCodec codec = new KafkaProtocolCodec();

        log.info("Create metadata buffer with pattern");
        byte[] pattern = new byte[] {(byte)0xDE, (byte)0xAD, (byte)0xBE, (byte)0xEF};
        ByteBuffer bb = ByteBuffer.allocate(100);
        for (int i = 0; i < numBytes; i++) {
            bb.put(i, pattern[i % pattern.length]);
        }
        JoinGroupRequest request = new JoinGroupRequest.Builder("group-id", 100, "member-id", "group",
                Lists.newArrayList(new JoinGroupRequest.ProtocolMetadata("foobar", bb))).build();
        RequestHeader header = new RequestHeader(ApiKeys.JOIN_GROUP, request.version(),
                                                 "foobar", 1234);
        ByteBuffer serialized = request.serialize(header);
        int size = serialized.remaining();

        log.info("Allocate buffer and simulate pooling");
        ByteBuf toDecode = Unpooled.buffer(size);
        toDecode.retain(); // take one ref for "pooling"
        toDecode.writeBytes(serialized);

        log.info("Decode buffer");
        List<Object> out = new ArrayList<Object>();
        codec.decode(mock(ChannelHandlerContext.class), toDecode, out);

        log.info("Release buffer, and if it has been 'returned' to the pool, overwrite");

        toDecode.release(); // release after decoding as netty would do
        if (toDecode.refCnt() == 1) { // if refcount is 1, then pooling could give out buffer again
             for (int i = 0; i < numBytes; i++) {
                toDecode.setByte(i, 0xFF);
            }
        }
        Assert.assertEquals(out.size(), 1);
        Assert.assertTrue(out.get(0) instanceof KafkaHeaderAndRequest);

        log.info("Validate fields");
        KafkaHeaderAndRequest har = (KafkaHeaderAndRequest)out.get(0);
        Assert.assertEquals(har.getHeader().apiKey(), ApiKeys.JOIN_GROUP);
        Assert.assertEquals(har.getHeader().apiVersion(), request.version());
        Assert.assertEquals(har.getHeader().clientId(), "foobar");
        Assert.assertEquals(har.getHeader().correlationId(), 1234);
        Assert.assertTrue(har.getRequest() instanceof JoinGroupRequest);
        JoinGroupRequest decoded = (JoinGroupRequest)har.getRequest();
        Assert.assertEquals(decoded.groupId(), "group-id");
        Assert.assertEquals(decoded.sessionTimeout(), 100);
        Assert.assertEquals(decoded.memberId(), "member-id");
        Assert.assertEquals(decoded.protocolType(), "group");
        Assert.assertEquals(decoded.groupProtocols().size(), 1);
        Assert.assertEquals(decoded.groupProtocols().get(0).name(), "foobar");
        ByteBuffer metadata = decoded.groupProtocols().get(0).metadata();

        log.info("If buffer has gone back to the pool, this will catch the corruption");
        for (int i = 0; i < numBytes; i++) {
            Assert.assertEquals(metadata.get(i), pattern[i % pattern.length]);
        }
    }

    @Test
    public void testBufferRefCounts() throws Exception {
        KafkaProtocolCodec codec = new KafkaProtocolCodec();

        ApiVersionsRequest request = (new ApiVersionsRequest.Builder()).build();
        RequestHeader header = new RequestHeader(ApiKeys.API_VERSIONS, request.version(),
                                                 "foobar", 1234);
        ByteBuffer serialized = request.serialize(header);
        int size = serialized.remaining();

        log.info("Generate buffer with message");
        ByteBuf buffer = Unpooled.buffer(size);
        buffer.writeBytes(serialized);

        log.info("Initial refCount should be 1");
        Assert.assertEquals(buffer.refCnt(), 1);
        List<Object> out = new ArrayList<Object>();

        log.info("Ref count incremented when passed to header and request");
        codec.decode(mock(ChannelHandlerContext.class), buffer, out);
        Assert.assertEquals(buffer.refCnt(), 2);

        log.info("Netty calls release after decode, so we do same");
        ReferenceCountUtil.release(buffer);
        Assert.assertEquals(buffer.refCnt(), 1);

        Assert.assertEquals(out.size(), 1);
        Assert.assertTrue(out.get(0) instanceof KafkaHeaderAndRequest);
        KafkaHeaderAndRequest decoded = (KafkaHeaderAndRequest)out.get(0);
        Assert.assertEquals(decoded.refCnt(), 1);

        log.info("Build a response for the request, and release the request");
        KafkaHeaderAndResponse response = KafkaHeaderAndResponse.responseForRequest(
                decoded, ApiVersionsResponse.defaultApiVersionsResponse());
        Assert.assertEquals(buffer.refCnt(), 2);
        ReferenceCountUtil.release(decoded);

        log.info("Buffer refcount is passed to the response");
        Assert.assertEquals(response.refCnt(), 1);
        Assert.assertEquals(buffer.refCnt(), 1);
        Assert.assertEquals(decoded.refCnt(), 0);

        log.info("Once response is released, buffer is also released");
        ReferenceCountUtil.release(response);
        Assert.assertEquals(response.refCnt(), 0);
        Assert.assertEquals(buffer.refCnt(), 0);
    }
}
