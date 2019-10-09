package io.streaml.msggw.kafka;

/*
 * Copyright 2019 Streamlio, Inc
 * Copyright 2019 The Apache Software Foundation
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

import java.nio.ByteBuffer;
import java.util.List;

import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.ApiVersionsRequest;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.ResponseHeader;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageCodec;
import io.netty.util.AbstractReferenceCounted;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class KafkaProtocolCodec extends MessageToMessageCodec<ByteBuf, KafkaProtocolCodec.KafkaHeaderAndResponse> {
    private static final Logger log = LoggerFactory.getLogger(KafkaProtocolCodec.class);

    @Override
    protected void encode(ChannelHandlerContext ctx,
                          KafkaHeaderAndResponse msg,
                          List<Object> out)
            throws Exception {
        ByteBuffer serialized = msg.getResponse().serialize(msg.getApiVersion(),
                                                            msg.getHeader());
        out.add(Unpooled.wrappedBuffer(serialized));
    }

    @Override
    protected void decode(ChannelHandlerContext ctx,
                          ByteBuf msg,
                          List<Object> out)
            throws Exception {
        if (msg.readableBytes() > 0) {
            ByteBuffer nio = msg.nioBuffer();

            RequestHeader header = RequestHeader.parse(nio);
            if (isUnsupportedApiVersionsRequest(header)) {
                // Unsupported ApiVersion requests are treated as v0 requests and are not parsed
                ApiVersionsRequest apiVersionsRequest = new ApiVersionsRequest((short) 0, header.apiVersion());
                out.add(new KafkaHeaderAndRequest(header, apiVersionsRequest, msg));
            } else {
                ApiKeys apiKey = header.apiKey();
                short apiVersion = header.apiVersion();
                Struct struct = apiKey.parseRequest(apiVersion, nio);
                AbstractRequest body = AbstractRequest.parseRequest(apiKey, apiVersion, struct);
                out.add(new KafkaHeaderAndRequest(header, body, msg));
            }
        }
    }

    private static boolean isUnsupportedApiVersionsRequest(RequestHeader header) {
        return header.apiKey() == API_VERSIONS && !API_VERSIONS.isVersionSupported(header.apiVersion());
    }


    static class KafkaHeaderAndResponse extends AbstractReferenceCounted {
        final private short apiVersion;
        final private ResponseHeader header;
        final private AbstractResponse response;
        final private ByteBuf buffer;

        private KafkaHeaderAndResponse(short apiVersion, ResponseHeader header,
                                       AbstractResponse response, ByteBuf buffer) {
            this.apiVersion = apiVersion;
            this.header = header;
            this.response = response;
            this.buffer = buffer.retain();
        }

        public short getApiVersion() {
            return apiVersion;
        }

        public ResponseHeader getHeader() {
            return header;
        }

        public AbstractResponse getResponse() {
            return response;
        }

        @Override
        public KafkaHeaderAndResponse touch(Object hint) {
            buffer.touch(hint);
            return this;
        }

        @Override
        protected void deallocate() {
            buffer.release();
        }

        static KafkaHeaderAndResponse responseForRequest(KafkaHeaderAndRequest request, AbstractResponse response) {
            return new KafkaHeaderAndResponse(request.getHeader().apiVersion(),
                                              request.getHeader().toResponseHeader(),
                                              response, request.getBuffer());
        }

        @Override
        public String toString() {
            return String.format("HeaderAndResponse(header=%s,response=%s)",
                                 header.toStruct().toString(), response.toString(getApiVersion()));
        }
    }

    static class KafkaHeaderAndRequest extends AbstractReferenceCounted {
        final private RequestHeader header;
        final private AbstractRequest request;
        final private ByteBuf buffer;

        KafkaHeaderAndRequest(RequestHeader header, AbstractRequest request, ByteBuf buffer) {
            this.header = header;
            this.request = request;
            this.buffer = buffer.retain();
        }

        public RequestHeader getHeader() {
            return header;
        }

        public AbstractRequest getRequest() {
            return request;
        }

        ByteBuf getBuffer() {
            return buffer;
        }

        @Override
        public KafkaHeaderAndRequest touch(Object hint) {
            buffer.touch(hint);
            return this;
        }

        @Override
        protected void deallocate() {
            buffer.release();
        }

        @Override
        public String toString() {
            return String.format("HeaderAndRequest(header=%s,request=%s)", header, request);
        }
    }
}

