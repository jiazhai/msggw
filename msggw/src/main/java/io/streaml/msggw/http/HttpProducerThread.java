package io.streaml.msggw.http;

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

import java.util.Enumeration;

import javax.servlet.http.HttpServletRequest;

import org.apache.pulsar.client.api.PulsarClient;

import com.github.benmanes.caffeine.cache.Ticker;

import io.streaml.msggw.AbstractProducerThread;

public class HttpProducerThread extends AbstractProducerThread<HttpProducerThread.RequestAndPayload> {

    public HttpProducerThread(PulsarClient pulsarClient) {
        super(pulsarClient, "http-producer-thread", 1024, 1024,
              Ticker.systemTicker(), 128, 300);
    }

    @Override
    public void translateMessage(RequestAndPayload request, MessageEvent event) {
        event.key = request.getRequest().getHeader(HttpService.PULSAR_HEADER_KEY);
        event.value = request.getPayload();

        Enumeration<String> properties = request.getRequest().getHeaders(HttpService.PULSAR_HEADER_PROPERTY);
        if (properties != null) {
            while (properties.hasMoreElements()) {
                String h = properties.nextElement();
                for (String e : h.split(",")) {
                    String[] parts = e.split(":", 2);
                    if (parts.length == 2) {
                        event.properties.put(parts[0], parts[1]);
                    }
                }
            }
        }
    }

    static class RequestAndPayload {
        final HttpServletRequest request;
        final byte[] payload;

        RequestAndPayload(HttpServletRequest request, byte[] payload) {
            this.request = request;
            this.payload = payload;
        }

        HttpServletRequest getRequest() {
            return request;
        }

        byte[] getPayload() {
            return payload;
        }
    }
}
