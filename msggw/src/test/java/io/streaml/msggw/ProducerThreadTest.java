package io.streaml.msggw;

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

import static io.streaml.msggw.MockPulsarCluster.USER1_TOKEN;
import static io.streaml.msggw.MockPulsarCluster.USER2_TOKEN;
import static io.streaml.msggw.MockPulsarCluster.USER3_TOKEN;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import com.github.benmanes.caffeine.cache.Ticker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ProducerThreadTest {
    private static final Logger log = LoggerFactory.getLogger(ProducerThreadTest.class);
    private MockPulsarCluster pulsar = null;

    @Before
    public void before() throws Exception {
        pulsar = new MockPulsarCluster();
        pulsar.startAsync().awaitRunning();
    }

    @After
    public void after() throws Exception {
        if (pulsar != null) {
            pulsar.stopAsync().awaitTerminated();
        }
    }

    @Test
    public void testPublish() throws Exception {
        String topic1 = "public/user1/topic1";
        String topic2 = "public/user2/topic2";
        TestProducerThread thread = createProducerThread();
        thread.startAsync().awaitRunning();

        try (PulsarClient client1 = pulsar.createPulsarClient("user1");
             PulsarClient client2 = pulsar.createPulsarClient("user2");
             Consumer<byte[]> consumer1 = client1
                .newConsumer(Schema.BYTES).topic(topic1)
                .subscriptionName("sub1").subscribe();
             Consumer<byte[]> consumer2 = client2
                .newConsumer(Schema.BYTES).topic(topic2)
                .subscriptionName("sub2").subscribe()) {
            CompletableFuture<MessageId> f1 = thread.publish(topic1, new KeyValue("k1", "v1", USER1_TOKEN));
            CompletableFuture<MessageId> f2 = thread.publish(topic2, new KeyValue("k2", "v2", USER2_TOKEN));
            CompletableFuture<MessageId> f3 = thread.publish(topic1, new KeyValue("k3", "v3", USER1_TOKEN));

            CompletableFuture.allOf(f1, f2, f3).get();

            Message<byte[]> m1 = consumer1.receive();
            assertThat(m1.getKey(), is("k1"));
            assertThat(new String(m1.getValue(), UTF_8), is("v1"));
            assertThat(m1.getMessageId(), is(f1.get()));

            Message<byte[]> m2 = consumer2.receive();
            assertThat(m2.getKey(), is("k2"));
            assertThat(new String(m2.getValue(), UTF_8), is("v2"));
            assertThat(m2.getMessageId(), is(f2.get()));

            Message<byte[]> m3 = consumer1.receive();
            assertThat(m3.getKey(), is("k3"));
            assertThat(new String(m3.getValue(), UTF_8), is("v3"));
            assertThat(m3.getMessageId(), is(f3.get()));

            CompletableFuture<MessageId> f4 = thread.publish(topic1, new KeyValue("k4", "v4", USER1_TOKEN));
            Message<byte[]> m4 = consumer1.receive();
            assertThat(m4.getKey(), is("k4"));
            assertThat(new String(m4.getValue(), UTF_8), is("v4"));
            assertThat(m4.getMessageId(), is(f4.get()));
        }

        thread.stopAsync().awaitTerminated();
    }

    @Test
    public void testBackpressure() throws Exception {
        String topic1 = "public/user1/topic1";
        int queueSize = 16;
        int maxPending = 16;
        TestProducerThread thread = createProducerThreadWithQueueSize(queueSize, maxPending);
        thread.startAsync().awaitRunning();
        int maxInFlight = queueSize + maxPending + 1;

        log.info("Block creation of producers in producer thread, to stall writes");
        CompletableFuture<Void> blockCreations = new CompletableFuture<Void>();
        thread.setCreateProducerHook(blockCreations);

        AtomicBoolean running = new AtomicBoolean(true);
        AtomicInteger produceCalls = new AtomicInteger(0);
        ForkJoinPool pool = new ForkJoinPool(1);
        try (PulsarClient client = pulsar.createPulsarClient("user1");
             Consumer<byte[]> consumer1 = client
                .newConsumer(Schema.BYTES).topic(topic1)
                .subscriptionName("sub1").subscribe()) {

            log.info("Create a thread writing to the producer thread in a tight loop");
            Future<?> writeThreadFuture = pool.submit(() -> {
                    while (running.get()) {
                        int i = produceCalls.getAndIncrement();
                        thread.publish(topic1, new KeyValue("k" + i, "v" + i, USER1_TOKEN));
                    }
                    return 0;
                });

            log.info("Thread should block, detect blocking by no change in 100ms");
            int lastProducedCount = -1;
            while (lastProducedCount != produceCalls.get()) {
                lastProducedCount = produceCalls.get();
                Thread.sleep(100);
            }
            int blockedAt = produceCalls.get();
            assertThat(blockedAt, greaterThanOrEqualTo(maxPending));
            assertThat(blockedAt, lessThanOrEqualTo(maxInFlight));

            log.info("No messages should have been published");
            assertThat(consumer1.receive(100, TimeUnit.MILLISECONDS), nullValue());

            log.info("Stop thread, and release producer");
            running.set(false);
            blockCreations.complete(null);
            assertThat(writeThreadFuture.get(), is(0));
            assertThat(blockedAt, is(produceCalls.get()));

            log.info("Consumer should receive all messages");
            for (int i = 0; i < blockedAt; i++) {
                Message<byte[]> m = consumer1.receive();
                assertThat(m.getKey(), is("k" + i));
                assertThat(new String(m.getValue(), UTF_8), is("v" + i));
            }

            log.info("And no more messages should have been published");
            assertThat(consumer1.receive(100, TimeUnit.MILLISECONDS), nullValue());

            log.info("Publish another message");
            thread.publish(topic1, new KeyValue("lastKey", "lastValue", USER1_TOKEN));

            Message<byte[]> m = consumer1.receive();
            assertThat(m.getKey(), is("lastKey"));
            assertThat(new String(m.getValue(), UTF_8), is("lastValue"));
        } finally {
            pool.shutdown();
        }

        thread.stopAsync().awaitTerminated();
    }

    @Test
    public void testThatCacheEvictsWhenFull() throws Exception {
        String topic1 = "public/user1/topic1";
        String topic2 = "public/user2/topic2";

        log.info("Create topics and ensure they have no producers");
        try (PulsarClient client1 = pulsar.createPulsarClient("user1");
             PulsarClient client2 = pulsar.createPulsarClient("user2")) {
            client1.newConsumer(Schema.BYTES).topic(topic1).subscriptionName("sub1").subscribe().close();
            client2.newConsumer(Schema.BYTES).topic(topic2).subscriptionName("sub1").subscribe().close();
        }
        assertThat(pulsar.getPulsarAdmin().topics().getStats(topic1).publishers, empty());
        assertThat(pulsar.getPulsarAdmin().topics().getStats(topic2).publishers, empty());

        MockTicker ticker = new MockTicker();
        TestProducerThread thread = createProducerThreadWithCacheParams(ticker,
                                                                        1 /* maxProducers */, 1 /* expirySecs */);
        thread.startAsync().awaitRunning();

        log.info("Publish to topic1 should create a producer");
        thread.publish(topic1, new KeyValue("k1", "v1", USER1_TOKEN)).get();
        assertThat(pulsar.getPulsarAdmin().topics().getStats(topic1).publishers, hasSize(1));
        assertThat(pulsar.getPulsarAdmin().topics().getStats(topic2).publishers, empty());

        log.info("Publishing to topic2 should push the first producer out");
        thread.publish(topic2, new KeyValue("k2", "v2", USER2_TOKEN)).get();
        // the close is async, so it may still linger a moment at the broker side after leaving the cache
        for (int i = 0; i < 10 && pulsar.getPulsarAdmin().topics().getStats(topic1).publishers.size() > 0; i++) {
            Thread.sleep(100);
        }
        assertThat(pulsar.getPulsarAdmin().topics().getStats(topic1).publishers, empty());
        assertThat(pulsar.getPulsarAdmin().topics().getStats(topic2).publishers, hasSize(1));

        thread.stopAsync().awaitTerminated();
    }

    @Test
    public void testThatCacheEvictsAfterTime() throws Exception {
        String topic1 = "public/user1/topic1";
        String topic2 = "public/user2/topic2";
        String topic3 = "public/user3/topic3";

        log.info("Create topics and ensure they have no producers");
        try (PulsarClient client1 = pulsar.createPulsarClient("user1");
             PulsarClient client2 = pulsar.createPulsarClient("user2");
             PulsarClient client3 = pulsar.createPulsarClient("user3")) {
            client1.newConsumer(Schema.BYTES).topic(topic1).subscriptionName("sub1").subscribe().close();
            client2.newConsumer(Schema.BYTES).topic(topic2).subscriptionName("sub1").subscribe().close();
            client3.newConsumer(Schema.BYTES).topic(topic3).subscriptionName("sub1").subscribe().close();
        }
        assertThat(pulsar.getPulsarAdmin().topics().getStats(topic1).publishers, empty());
        assertThat(pulsar.getPulsarAdmin().topics().getStats(topic2).publishers, empty());
        assertThat(pulsar.getPulsarAdmin().topics().getStats(topic3).publishers, empty());

        MockTicker ticker = new MockTicker();
        TestProducerThread thread = createProducerThreadWithCacheParams(ticker,
                                                                        2 /* maxProducers */, 1 /* expirySecs */);
        thread.startAsync().awaitRunning();

        log.info("Publish to topic1 & topic2 should create a producer for each");
        thread.publish(topic1, new KeyValue("k1", "v1", USER1_TOKEN)).get();
        thread.publish(topic2, new KeyValue("k2", "v2", USER2_TOKEN)).get();
        assertThat(pulsar.getPulsarAdmin().topics().getStats(topic1).publishers, hasSize(1));
        assertThat(pulsar.getPulsarAdmin().topics().getStats(topic2).publishers, hasSize(1));
        assertThat(pulsar.getPulsarAdmin().topics().getStats(topic3).publishers, empty());

        log.info("Advance past the expiry (1sec)");
        ticker.advance(2, TimeUnit.SECONDS);

        log.info("Publishing to topic3, both other producers should be closed because of expiry");
        thread.publish(topic3, new KeyValue("k3", "v3", USER3_TOKEN)).get();
        // the close is async, so it may still linger a moment at the broker side after leaving the cache
        for (int i = 0; i < 10
                 && pulsar.getPulsarAdmin().topics().getStats(topic1).publishers.size() > 0
                 && pulsar.getPulsarAdmin().topics().getStats(topic2).publishers.size() > 0; i++) {
            Thread.sleep(100);
        }
        assertThat(pulsar.getPulsarAdmin().topics().getStats(topic1).publishers, empty());
        assertThat(pulsar.getPulsarAdmin().topics().getStats(topic2).publishers, empty());
        assertThat(pulsar.getPulsarAdmin().topics().getStats(topic3).publishers, hasSize(1));

        thread.stopAsync().awaitTerminated();
    }

    static class KeyValue {
        final String key;
        final String value;
        final String token;

        KeyValue(String key, String value, String token) {
            this.key = key;
            this.value = value;
            this.token = token;
        }
    }

    static class MockTicker implements Ticker {
        long tick;

        void advance(long duration, TimeUnit unit) {
            tick += unit.toNanos(duration);
        }

        @Override
        public long read() {
            return tick;
        }
    }

    TestProducerThread createProducerThread() throws Exception {
        return new TestProducerThread(1024, 1024, Ticker.systemTicker(), 200, Integer.MAX_VALUE);
    }

    TestProducerThread createProducerThreadWithQueueSize(int queueSize, int maxPending)
            throws Exception {
        return new TestProducerThread(queueSize, maxPending, Ticker.systemTicker(), 200, Integer.MAX_VALUE);
    }

    TestProducerThread createProducerThreadWithCacheParams(Ticker ticker, int maxProducerCache, int expirySecs)
            throws Exception {
        return new TestProducerThread(1024, 1024, ticker, maxProducerCache, expirySecs);
    }

    class TestProducerThread extends AbstractProducerThread<KeyValue> {
        private volatile CompletableFuture<Void> createProducerHook = CompletableFuture.completedFuture(null);


        TestProducerThread(int queueSize, int maxPending,
                           Ticker ticker, int maxProducerCache, int producerCacheExpiry)
                throws Exception {
            super(pulsar.createSuperUserClient(), "test-producer-thread",
                  queueSize, maxPending, ticker, maxProducerCache, producerCacheExpiry);
        }

        public void setCreateProducerHook(CompletableFuture<Void> hook) {
            this.createProducerHook = hook;
        }

        @Override
        public void translateMessage(KeyValue message, MessageEvent event) {
            event.key = message.key;
            event.value = message.value.getBytes(UTF_8);
        }

        @Override
        public CompletableFuture<Producer<byte[]>> createProducer(String topic) {
            return createProducerHook.thenCompose((ignore) -> {
                    return super.createProducer(topic);
                });
        }
    }
}
