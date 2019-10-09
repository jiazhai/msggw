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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

import com.github.benmanes.caffeine.cache.Ticker;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.MoreExecutors;

import io.streaml.msggw.MessagingGatewayConfiguration;
import io.streaml.msggw.MockPulsarCluster;

import java.util.Base64;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.pulsar.broker.authentication.AuthenticationService;
import org.apache.pulsar.broker.authorization.AuthorizationService;
import org.apache.pulsar.broker.cache.ConfigurationCacheService;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.PulsarClient;

import org.apache.pulsar.zookeeper.GlobalZooKeeperCache;

import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.logging.LoggingFeature;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.Ignore;

public class HttpTest {

    private static final Logger log = LoggerFactory.getLogger(HttpTest.class);

    private MockPulsarCluster pulsar = null;
    private HttpProducerThread producerThread = null;
    private HttpService httpService = null;
    private Client httpClient = null;

    private ScheduledExecutorService scheduler;
    private GlobalZooKeeperCache globalZkCache;
    private ConfigurationCacheService configurationCacheService;
    private AuthorizationService authorizationService;
    private AuthenticationService authenticationService;

    @Before
    public void before() throws Exception {
        pulsar = new MockPulsarCluster();
        pulsar.startAsync().awaitRunning();

        MessagingGatewayConfiguration conf = new MessagingGatewayConfiguration();
        conf.setWebServicePort(Optional.of(0));
        conf.setBrokerServiceURL(pulsar.lookupUrl());
        conf.setBrokerWebServiceURL(pulsar.httpServiceUrl());
        conf.setHttpNumThreads(32);

        conf.setWebSocketProxyEnabled(false);
        conf.setAuthenticationEnabled(true);
        conf.setAuthorizationEnabled(true);
        conf.setSuperUserRoles(Sets.newHashSet("admin"));
        conf.setAuthenticationProviders(
                Sets.newHashSet("org.apache.pulsar.broker.authentication.AuthenticationProviderToken"));
        conf.setBrokerClientAuthenticationPlugin("org.apache.pulsar.client.impl.auth.AuthenticationToken");
        conf.setBrokerClientAuthenticationParameters(MockPulsarCluster.ADMIN_TOKEN);
        conf.getProperties().setProperty("tokenSecretKey", MockPulsarCluster.SECRET_KEY);

        scheduler = Executors.newSingleThreadScheduledExecutor();
        globalZkCache = new GlobalZooKeeperCache(pulsar.getZooKeeperClientFactory(),
                30000, 3000, "localhost:2181", pulsar.getExecutor(), scheduler);
        globalZkCache.start();
        configurationCacheService = new ConfigurationCacheService(globalZkCache);
        authorizationService = new AuthorizationService(conf.toServiceConfiguration(),
                                                        configurationCacheService);

        authenticationService = new AuthenticationService(conf.toServiceConfiguration());
        producerThread = new HttpProducerThread(pulsar.createSuperUserClient());
        httpService = new HttpService(conf, MoreExecutors.newDirectExecutorService(),
                                      authenticationService,
                                      Optional.of(authorizationService),
                                      producerThread);
        producerThread.startAsync().awaitRunning();
        httpService.startAsync().awaitRunning();

        httpClient = ClientBuilder.newClient(new ClientConfig().register(LoggingFeature.class));
    }

    @After
    public void after() throws Exception {
        if (authenticationService != null) {
            authenticationService.close();
        }
        if (globalZkCache != null) {
            globalZkCache.stop();
        }
        if (scheduler != null) {
            scheduler.shutdownNow();
        }
        if (pulsar != null) {
            pulsar.stopAsync().awaitTerminated();
        }
        if (httpService != null) {
            httpService.stopAsync().awaitTerminated();
        }
        if (producerThread != null) {
            producerThread.stopAsync().awaitTerminated();
        }
        pulsar = null;
        httpService = null;
        producerThread = null;
    }

    @Test
    public void testHttpProduce() throws Exception {
        try (PulsarClient client = pulsar.createPulsarClient("admin")) {
            testHttpProduce(client, httpClient.target(httpService.getServiceUri()));
        }
    }

    @Ignore
    public void testHttpProducerWithQueryString() throws Exception {
        try (PulsarClient client = pulsar.createPulsarClient("user1");
             Consumer<String> consumer = client.newConsumer(Schema.STRING)
                .subscriptionName("test").topic("public/user1/http-test").subscribe()) {
            Response r = httpClient.target(httpService.getServiceUri())
                .path("/data/v1/topics/public/user1/http-test")
                .queryParam("query", "junk").request()
                .header("Authorization", "Bearer " + MockPulsarCluster.USER1_TOKEN)
                .post(Entity.entity("foobar", MediaType.APPLICATION_OCTET_STREAM));
            assertThat(r.getStatus(), is(Response.Status.OK.getStatusCode()));

            TopicServlet.MsgId publishedMsgId = r.readEntity(TopicServlet.MsgId.class);
            Message<String> msg = consumer.receive();
            assertThat(msg.getValue(), is("foobar"));
            assertThat(Base64.getEncoder().encodeToString(msg.getMessageId().toByteArray()),
                       is(publishedMsgId.getMessageId()));
        }
    }

    static void testHttpProduce(PulsarClient pulsarClient, WebTarget webTarget) throws Exception {
        try (Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .subscriptionName("test").topic("public/user1/http-test").subscribe()) {
            Response r = webTarget
                .path("/data/v1/topics/public/user1/http-test").request()
                .header("Authorization", "Bearer " + MockPulsarCluster.USER1_TOKEN)
                .header("X-Pulsar-Key", "my-key")
                .header("X-Pulsar-Property", "foo:bar")
                .header("X-Pulsar-Property", "baz:foo")
                .post(Entity.entity("foobar", MediaType.APPLICATION_OCTET_STREAM));
            assertThat(r.getStatus(), is(Response.Status.OK.getStatusCode()));

            TopicServlet.MsgId publishedMsgId = r.readEntity(TopicServlet.MsgId.class);
            Message<String> msg = consumer.receive();
            assertThat(msg.getValue(), is("foobar"));
            assertThat(msg.getKey(), is("my-key"));
            assertThat(msg.getProperty("foo"), is("bar"));
            assertThat(msg.getProperty("baz"), is("foo"));
            assertThat(Base64.getEncoder().encodeToString(msg.getMessageId().toByteArray()),
                       is(publishedMsgId.getMessageId()));
        }
    }
}
