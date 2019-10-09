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

import java.io.IOException;
import java.util.Base64;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import javax.servlet.AsyncContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.HttpMethod;
import javax.ws.rs.core.MediaType;

import org.apache.commons.io.IOUtils;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import org.apache.pulsar.broker.authorization.AuthorizationService;
import org.apache.pulsar.broker.web.AuthenticationFilter;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.util.ObjectMapperFactory;

import io.prometheus.client.Histogram.Timer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TopicServlet extends HttpServlet {
    private static final Logger log = LoggerFactory.getLogger(TopicServlet.class);
    private static final long serialVersionUID = 1L;

    private final HttpProducerThread producerThread;
    private final Optional<AuthorizationService> authorization;

    public TopicServlet(HttpProducerThread producerThread,
                        Optional<AuthorizationService> authorization) {
        this.producerThread = producerThread;
        this.authorization = authorization;
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        // Path will received here as in "/persistent://public/default/my-topic"
        // Ignore the first '/'
        String topic = req.getPathInfo().substring(1);
        TopicName topicName = TopicName.get(topic);

        String role = (String)req.getAttribute(AuthenticationFilter.AuthenticatedRoleAttributeName);
        AuthenticationDataSource authData =
            (AuthenticationDataSource)req.getAttribute(AuthenticationFilter.AuthenticatedDataAttributeName);
        Timer timer = Metrics.registerOpStart(HttpMethod.POST, topicName);
        byte[] payload = IOUtils.toByteArray(req.getInputStream());
        Metrics.registerOpBytes(HttpMethod.POST, topicName, payload.length);
        AsyncContext ctx = req.startAsync(req, resp);
        resp.setContentType(MediaType.APPLICATION_JSON);

        authorization.map(service -> service.canProduceAsync(topicName, role, authData))
            .orElse(CompletableFuture.completedFuture(true))
            .whenComplete((authorized, exception) -> {
                    if (exception != null) {
                        if (log.isDebugEnabled()) {
                            log.debug("Exception handling post to {}", topic, exception);
                        }
                        Metrics.registerOpError(HttpMethod.POST, topicName);
                        resp.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
                        timer.observeDuration();
                        ctx.complete();
                    } else if (!authorized) {
                        Metrics.registerOpError(HttpMethod.POST, topicName);
                        resp.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
                        timer.observeDuration();
                        ctx.complete();
                    } else {
                        producerThread.publish(topic, new HttpProducerThread.RequestAndPayload(req, payload))
                            .whenComplete((msgId, exception2) -> {
                                    if (exception2 != null) {
                                        if (log.isDebugEnabled()) {
                                            log.debug("Exception handling post to {}", topic, exception2);
                                        }
                                        Metrics.registerOpError(HttpMethod.POST, topicName);
                                        resp.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
                                    } else {
                                        try {
                                            ObjectMapperFactory.getThreadLocal().writeValue(
                                                    resp.getOutputStream(), new MsgId(msgId));
                                            resp.setStatus(HttpServletResponse.SC_OK);
                                        } catch (IOException e) {
                                            if (log.isDebugEnabled()) {
                                                log.debug("Exception handling post to {}", topic, e);
                                            }
                                            resp.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
                                        }
                                    }
                                    timer.observeDuration();
                                    ctx.complete();
                                });
                    }
                });
    }

    static class MsgId {
        String messageId;

        MsgId() {
        }

        MsgId(MessageId msgId) {
            messageId = Base64.getEncoder().encodeToString(msgId.toByteArray());
        }

        public String getMessageId() {
            return messageId;
        }
    }
}
