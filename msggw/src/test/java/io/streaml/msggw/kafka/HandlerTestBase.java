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

import static org.mockito.Mockito.spy;

import com.google.common.base.Ticker;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import io.netty.util.Timer;
import io.netty.util.HashedWheelTimer;
import io.streaml.conhash.CHashGroup;
import io.streaml.msggw.MockPulsarCluster;

import java.net.ServerSocket;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.pulsar.client.api.PulsarClient;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.junit.After;
import org.junit.Before;

public class HandlerTestBase {

    static final Logger log = LoggerFactory.getLogger(HandlerTestBase.class);

    MockPulsarCluster pulsar = null;
    PulsarClient pulsarClient = null;
    KafkaService kafka = null;
    KafkaProducerThread producerThread = null;
    int kafkaPort;
    ScheduledExecutorService scheduler = null;

    Timer timer = null;
    ConsumerGroupsImpl groups = null;
    ExecutorService groupsExecutor = null;
    MockConsumerGroupStorage storage = spy(new MockConsumerGroupStorage());

    static int selectPort() throws Exception {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }

    @Before
    public void before() throws Exception {
        kafkaPort = selectPort();
        pulsar = new MockPulsarCluster();
        pulsar.startAsync().awaitRunning();

        scheduler = Executors.newScheduledThreadPool(1,
                new ThreadFactoryBuilder().setNameFormat("test-").setUncaughtExceptionHandler(
                        (t, e) -> {
                            log.error("Caught exception in thread {}", t, e);
                        }).build());
        groupsExecutor = Executors.newSingleThreadExecutor(
                new ThreadFactoryBuilder().setNameFormat("groups-").setUncaughtExceptionHandler(
                        (t, e) -> {
                            log.error("Caught exception in thread {}", t, e);
                        }).build());
        timer = new HashedWheelTimer();
        pulsarClient = pulsar.createSuperUserClient();

        startHandler();
    }

    protected void startHandler() throws Exception {
        String advertizedAddress = "localhost:" + kafkaPort;
        CHashGroup chashGroup = new MockCHashGroup(advertizedAddress);

        Fetcher fetcher = new FetcherImpl(pulsarClient, timer);
        groups = spy(new ConsumerGroupsImpl(chashGroup,
                                            Ticker.systemTicker(), storage,
                                            groupsExecutor));
        producerThread = new KafkaProducerThread(pulsar.createSuperUserClient());

        kafka = new KafkaService(kafkaPort, scheduler, groups,
                new KafkaRequestHandler("test",
                                        new NodeIds() {
                                            @Override
                                            public CompletableFuture<Integer> idForNode(String node) {
                                                return CompletableFuture.completedFuture(1);
                                            }
                                        },
                                        chashGroup,
                                        pulsar.getPulsarAdmin(),
                                        pulsarClient,
                                        scheduler, producerThread,
                                        fetcher, groups,
                                        pulsar.getTokenAuthnProvider(),
                                        Optional.ofNullable(pulsar.getAuthzService())));
        producerThread.startAsync().awaitRunning();
        kafka.startAsync().awaitRunning();
    }

    protected void stopHandler() throws Exception {
        if (kafka != null) {
            kafka.stopAsync().awaitTerminated();
        }
        if (producerThread != null) {
            producerThread.stopAsync().awaitTerminated();
        }
        kafka = null;
        producerThread = null;
    }

    protected void restartHandler() throws Exception {
        stopHandler();
        startHandler();
    }

    @After
    public void after() throws Exception {
        stopHandler();

        if (pulsarClient != null) {
            pulsarClient.close();
        }
        if (pulsar != null) {
            pulsar.stopAsync().awaitTerminated();
        }
        if (scheduler != null) {
            scheduler.shutdownNow();
        }
        if (groupsExecutor != null) {
            scheduler.shutdownNow();
        }
        if (timer != null) {
            timer.stop();
        }
        pulsar = null;
        scheduler = null;
    }

    AdminClient kafkaAdminClient() {
        Properties props = consumerProperties();

        AdminClient admin = AdminClient.create(props);
        return admin;
    }

    Properties producerProperties() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:" + kafkaPort);
        props.put("retries", 0);
        props.put("enable.auto.commit", "false");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        props.put("security.protocol", "SASL_PLAINTEXT");
        props.put("sasl.mechanism", "PLAIN");
        props.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required"
                  + " username=\"public/user1\" password=\"" + pulsar.USER1_TOKEN + "\";");

        return props;
    }

    Properties consumerProperties() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:" + kafkaPort);
        props.put("retries", 0);
        props.put("enable.auto.commit", "false");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("partition.assignment.strategy", "org.apache.kafka.clients.consumer.RoundRobinAssignor");

        props.put("security.protocol", "SASL_PLAINTEXT");
        props.put("sasl.mechanism", "PLAIN");
        props.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required"
                  + " username=\"public/user1\" password=\"" + pulsar.USER1_TOKEN + "\";");

        return props;
    }

}
