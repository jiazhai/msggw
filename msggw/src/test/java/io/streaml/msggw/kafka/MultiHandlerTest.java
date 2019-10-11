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

import com.google.common.base.Ticker;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timer;
import io.streaml.conhash.CHashGroupService;
import io.streaml.conhash.CHashGroupZKImpl;
import io.streaml.msggw.MessagingGatewayConfiguration;
import io.streaml.msggw.MockPulsarCluster;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.pulsar.client.api.PulsarClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyObject;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

public class MultiHandlerTest {
    static final Logger log = LoggerFactory.getLogger(MultiHandlerTest.class);
    private static final int NUM_HANDLERS = 5;
    MockPulsarCluster pulsar = null;
    PulsarClient pulsarClient = null;
    ScheduledExecutorService scheduler = null;

    List<Handler> handlers = new ArrayList<>();
    Timer timer = null;
    ExecutorService groupsExecutor = null;
    MockConsumerGroupStorage storage = spy(new MockConsumerGroupStorage());

    static int selectPort() throws Exception {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }

    private class Handler {
        final int kafkaPort;
        CHashGroupService chashGroup = null;
        ConsumerGroupsImpl groups = null;
        KafkaService kafka = null;
        KafkaProducerThread producerThread = null;
        KafkaProducerThread spiedProducerThread = null;

        Handler() throws Exception {
            kafkaPort = selectPort();
        }

        int ownerCount() {
            log.info("owners {}", chashGroup.currentState().allOwners());
            return chashGroup.currentState().allOwners().size();
        }

        void start() throws Exception {
            Fetcher fetcher = new FetcherImpl(pulsarClient, timer);
            KafkaProducerThread producerThread = new KafkaProducerThread(pulsar.createSuperUserClient());
            spiedProducerThread = spy(producerThread);
            chashGroup = new CHashGroupZKImpl(toString(), "/kafka", pulsar.getZooKeeperClient(),
                                              groupsExecutor, (e) -> log.error("FATAL: error in chashgroup", e));
            groups = spy(new ConsumerGroupsImpl(chashGroup, Ticker.systemTicker(),
                                                storage, groupsExecutor));
            NodeIds nodeIds = new NodeIdsImpl(scheduler, pulsar.getZooKeeperClient(), "/kafka-nodes");
            kafka = new KafkaService(kafkaPort, scheduler, groups,
                    new KafkaRequestHandler("test",
                            nodeIds,
                            chashGroup,
                            pulsar.getPulsarAdmin(),
                            pulsarClient,
                            scheduler, spiedProducerThread,
                            fetcher, groups,
                            pulsar.getAuthService(),
                            Optional.ofNullable(pulsar.getAuthzService()),
                            MessagingGatewayConfiguration.fromServiceConfiguration(pulsar.getServiceConfiguration())));
            chashGroup.startAsync().awaitRunning();
            producerThread.startAsync().awaitRunning();
            kafka.startAsync().awaitRunning();
        }

        void stop() throws Exception {
            if (kafka != null) {
                kafka.stopAsync().awaitTerminated();
            }
            if (producerThread != null) {
                producerThread.stopAsync().awaitTerminated();
            }
            if (chashGroup != null) {
                chashGroup.stopAsync().awaitTerminated();
            }
            kafka = null;
            producerThread = null;
        }

        @Override
        public String toString() {
            return "localhost:" + kafkaPort;
        }
    }

    @Before
    public void before() throws Exception {
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

        for (int i = 0; i < NUM_HANDLERS; i++) {
            Handler h = new Handler();
            handlers.add(h);
            h.start();
        }

        // wait for members to see members
        for (Handler h : handlers) {
            for (int i = 0; i < 100; i++) {
                if (h.ownerCount() == handlers.size()) {
                    break;
                }
                Thread.sleep(100);
            }
            assertThat(h.ownerCount(), equalTo(handlers.size()));
        }
    }

    @After
    public void after() throws Exception {
        for (Handler h : handlers) {
            h.stop();
        }

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

    @Test(timeout=60000)
    public void testTopicAssignment() throws Exception {
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProperties())) {
            for (int i = 0; i < 100; i++) {
                producer.send(new ProducerRecord<>("topic"+i, "key1", "value1"));
            }
            producer.flush();
        }
        for (Handler h : handlers) {
            verify(h.spiedProducerThread, atLeastOnce()).publish(anyString(), anyObject());
        }
    }

    @Test(timeout=60000)
    public void testGroupAssignment() throws Exception {
        int numConsumers = NUM_HANDLERS*10;
        ExecutorService consumerExecutor = Executors.newFixedThreadPool(numConsumers,
                new ThreadFactoryBuilder().setNameFormat("groups-").setUncaughtExceptionHandler(
                        (t, e) -> {
                            log.error("Caught exception in thread {}", t, e);
                        }).build());
        try {
            List<Future<?>> futures = IntStream.range(0, numConsumers)
                .mapToObj(i -> {
                        Properties props = consumerProperties();
                        props.put("group.id", "group"+i);
                        return consumerExecutor.submit(() -> {
                                try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
                                    consumer.subscribe(Lists.newArrayList("foobar"));
                                    consumer.poll(5000);
                                }
                            });
                    }).collect(Collectors.toList());
            for (Future<?> f : futures) {
                f.get(10, TimeUnit.SECONDS);
            }
        } finally {
            consumerExecutor.shutdownNow();
        }

        for (Handler h : handlers) {
            verify(h.groups, atLeastOnce()).joinGroup(anyString(), anyString(), anyString(), anyInt(), anyInt(), anyObject());
        }
    }

    Properties producerProperties() {
        Properties props = new Properties();
        props.put("bootstrap.servers", handlers.stream().map(Object::toString).collect(Collectors.joining(",")));
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
        props.put("bootstrap.servers", handlers.stream().map(Object::toString).collect(Collectors.joining(",")));
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
