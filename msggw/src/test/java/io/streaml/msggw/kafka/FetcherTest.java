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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;

import io.streaml.msggw.MockPulsarCluster;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.TopicPartition;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.ReaderBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.Ignore;

public class FetcherTest {

    private static final Logger log = LoggerFactory.getLogger(FetcherTest.class);
    private static final String NAMESPACE = "public/user1";
    private MockPulsarCluster pulsar = null;
    private MockTimer timer = null;
    private PulsarClient pulsarClient = null;

    @Before
    public void before() throws Exception {
        pulsar = new MockPulsarCluster();
        pulsar.startAsync().awaitRunning();

        timer = new MockTimer();

        pulsarClient = pulsar.createSuperUserClient();
    }

    @After
    public void after() throws Exception {
        if (pulsar != null) {
            pulsar.stopAsync().awaitTerminated();
        }
        if (timer != null) {
            timer.stop();
        }
    }

    @Test
    public void testFetchFromStart() throws Exception {
        Fetcher f = new FetcherImpl(pulsarClient, timer);

        TopicPartition topic = new TopicPartition("fetch-start", 0);

        // timeout is infinite, fetch returns when we have seen more bytes than
        // the minimum which is the length of "foobar"
        CompletableFuture<Map<TopicPartition, ? extends Fetcher.Result>> result =
            f.fetch(NAMESPACE, ImmutableMap.of(topic, 0L), (byteCount) -> byteCount >= "foobar".getBytes(UTF_8).length,
                    0, TimeUnit.SECONDS);

        try (Producer<String> p = pulsarClient.newProducer(Schema.STRING)
                .topic(NAMESPACE + "/" + topic.topic()).create()) {
            p.newMessage().key("blah").value("foobar").send();
        }

        Map<TopicPartition, ? extends Fetcher.Result> messages = result.get();
        assertThat(messages.keySet(), contains(topic));
        assertThat(messages.get(topic).getError().isPresent(), is(false));
        assertThat(messages.get(topic).getMessages(), hasSize(1));
        assertThat(messages.get(topic).getMessages().get(0).getKey(), is("blah"));
        assertThat(new String(messages.get(topic).getMessages().get(0).getData(), UTF_8), is("foobar"));

        long newOffset = MessageIdUtils.getOffset(messages.get(topic).getMessages().get(0).getMessageId()) + 1;
        result = f.fetch(NAMESPACE, ImmutableMap.of(topic, newOffset), (byteCount) -> byteCount >= "blah".getBytes(UTF_8).length,
                          0, TimeUnit.SECONDS);

        try (Producer<String> p = pulsarClient.newProducer(Schema.STRING)
                .topic(NAMESPACE + "/" + topic.topic()).create()) {
            p.newMessage().key("foo").value("blah").send();
        }

        messages = result.get();
        assertThat(messages.keySet(), contains(topic));
        assertThat(messages.get(topic).getError().isPresent(), is(false));
        assertThat(messages.get(topic).getMessages(), hasSize(1));
        assertThat(messages.get(topic).getMessages().get(0).getKey(), is("foo"));
        assertThat(new String(messages.get(topic).getMessages().get(0).getData(), UTF_8), is("blah"));
    }

    @Test
    public void testFetchMultipleTopics() throws Exception {
        Fetcher f = new FetcherImpl(pulsarClient, timer);

        TopicPartition topic1 = new TopicPartition("topic1", 0);
        TopicPartition topic2 = new TopicPartition("topic2", 0);

        CompletableFuture<Map<TopicPartition, ? extends Fetcher.Result>> result =
            f.fetch(NAMESPACE, ImmutableMap.of(topic1, 0L, topic2, 0L),
                    (byteCount) -> byteCount >= "foobar".getBytes(UTF_8).length * 2,
                    0, TimeUnit.SECONDS);

        try (Producer<String> p1 = pulsarClient.newProducer(Schema.STRING)
                .topic(NAMESPACE + "/" + topic1.topic()).create();
             Producer<String> p2 = pulsarClient.newProducer(Schema.STRING)
                .topic(NAMESPACE + "/" + topic2.topic()).create()) {
            p1.newMessage().key("blah1").value("foobar").send();
            p2.newMessage().key("blah2").value("barfoo").send();
        }

        Map<TopicPartition, ? extends Fetcher.Result> messages = result.get();
        assertThat(messages.keySet(), contains(topic1, topic2));
        assertThat(messages.get(topic1).getError().isPresent(), is(false));
        assertThat(messages.get(topic2).getError().isPresent(), is(false));
        assertThat(messages.get(topic1).getMessages(), hasSize(1));
        assertThat(messages.get(topic2).getMessages(), hasSize(1));

        Map<String, String> values = ImmutableMap.of(
                messages.get(topic1).getMessages().get(0).getKey(),
                new String(messages.get(topic1).getMessages().get(0).getData(), UTF_8),
                messages.get(topic2).getMessages().get(0).getKey(),
                new String(messages.get(topic2).getMessages().get(0).getData(), UTF_8));
        assertThat(values, hasEntry("blah1", "foobar"));
        assertThat(values, hasEntry("blah2", "barfoo"));
    }

    @Test
    public void testFetchMaxBytes() throws Exception {
        Fetcher f = new FetcherImpl(pulsarClient, timer);

        TopicPartition topic = new TopicPartition("fetch-max-bytes", 0);
        // max bytes is 1, timeout is 10 minutes. fetch should return when we have more than 1 byte
        CompletableFuture<Map<TopicPartition, ? extends Fetcher.Result>> result =
            f.fetch(NAMESPACE, ImmutableMap.of(topic, 0L), (byteCount) -> byteCount > 1,
                    10, TimeUnit.MINUTES);

        try (Producer<String> p = pulsarClient.newProducer(Schema.STRING)
                .topic(NAMESPACE + "/" + topic.topic()).create()) {
            p.newMessage().key("blah").value("foobar").send();
        }

        Map<TopicPartition, ? extends Fetcher.Result> messages = result.get();
        assertThat(messages.keySet(), contains(topic));
        assertThat(messages.get(topic).getError().isPresent(), is(false));
        assertThat(messages.get(topic).getMessages(), hasSize(1));
        assertThat(messages.get(topic).getMessages().get(0).getKey(), is("blah"));
        assertThat(new String(messages.get(topic).getMessages().get(0).getData(), UTF_8), is("foobar"));
    }

    @Test
    public void testFetchTimeout() throws Exception {
        Fetcher f = new FetcherImpl(pulsarClient, timer);

        TopicPartition topic = new TopicPartition("topic1", 0);
        CompletableFuture<Map<TopicPartition, ? extends Fetcher.Result>> result =
            f.fetch(NAMESPACE, ImmutableMap.of(topic, 0L), (byteCount) -> false, 10, TimeUnit.SECONDS);
        try (Producer<String> p = pulsarClient.newProducer(Schema.STRING)
                .topic(NAMESPACE + "/" + topic.topic()).create()) {
            p.newMessage().key("blah").value("foobar").send();
        }
        Thread.sleep(1000);
        timer.advanceClock(10, TimeUnit.SECONDS);
        Map<TopicPartition, ? extends Fetcher.Result> messages = result.get();

        assertThat(messages.keySet(), contains(topic));
        assertThat(messages.get(topic).getError().isPresent(), is(false));
        assertThat(messages.get(topic).getMessages(), hasSize(1));
        assertThat(messages.get(topic).getMessages().get(0).getKey(), is("blah"));
        assertThat(new String(messages.get(topic).getMessages().get(0).getData(), UTF_8), is("foobar"));
    }

    @Test
    public void testFetchAllErrors() throws Exception {
        PulsarClient mockClient = spy(pulsarClient);
        when(mockClient.newReader()).thenAnswer((invokation) -> {
                ReaderBuilder<byte[]> builder = spy(pulsarClient.newReader());

                when(builder.createAsync()).thenAnswer((invokation2) -> {
                        CompletableFuture<Reader> exception = new CompletableFuture<>();
                        exception.completeExceptionally(new PulsarClientException("foobar"));
                        return exception;
                    });
                return builder;
            });
        Fetcher f = new FetcherImpl(mockClient, timer);

        TopicPartition topic = new TopicPartition("topic", 0);
        CompletableFuture<Map<TopicPartition, ? extends Fetcher.Result>> result =
            f.fetch(NAMESPACE, ImmutableMap.of(topic, 0L), (byteCount) -> false, 10, TimeUnit.SECONDS);
        try (Producer<String> p = pulsarClient.newProducer(Schema.STRING)
                .topic(NAMESPACE + "/" + topic.topic()).create()) {
            p.newMessage().key("blah").value("foobar").send();
        }

        Map<TopicPartition, ? extends Fetcher.Result> messages = result.get();
        assertThat(messages.keySet(), contains(topic));
        assertThat(messages.get(topic).getError().isPresent(), is(true));
        assertThat(messages.get(topic).getError().get(), instanceOf(PulsarClientException.class));
    }

    @Test
    public void testFetchSomeGoodSomeErrors() throws Exception {
        TopicPartition topic1 = new TopicPartition("topic1", 0);
        TopicPartition topic2 = new TopicPartition("topic2", 0);

        PulsarClient mockClient = spy(pulsarClient);
        when(mockClient.newReader()).thenAnswer((invokation) -> {
                ReaderBuilder<byte[]> builder = spy(pulsarClient.newReader());

                when(builder.createAsync()).thenAnswer((invokation2) -> {
                        CompletableFuture<Reader> promise = new CompletableFuture<>();
                        CompletableFuture<Reader> future = (CompletableFuture<Reader>)invokation2.callRealMethod();
                        // can't just do thenCompose as that wraps the exception in CompletionException
                        future.whenComplete((reader, exception) -> {
                                if (exception != null) {
                                    promise.completeExceptionally(exception);
                                } else if (reader.getTopic().endsWith(topic1.topic())) {
                                    promise.completeExceptionally(new PulsarClientException("foobar"));
                                } else {
                                    promise.complete(reader);
                                }
                            });
                        return promise;
                    });
                return builder;
            });
        Fetcher f = new FetcherImpl(mockClient, timer);

        CompletableFuture<Map<TopicPartition, ? extends Fetcher.Result>> result =
            f.fetch(NAMESPACE, ImmutableMap.of(topic1, 0L, topic2, 0L),
                    (byteCount) -> byteCount >= "foobar".getBytes(UTF_8).length, 10, TimeUnit.SECONDS);
        try (Producer<String> p = pulsarClient.newProducer(Schema.STRING)
                .topic(NAMESPACE + "/" + topic2.topic()).create()) {
            p.newMessage().key("blah").value("foobar").send();
        }

        Map<TopicPartition, ? extends Fetcher.Result> messages = result.get();
        assertThat(messages.keySet(), contains(topic1, topic2));
        assertThat(messages.get(topic1).getError().isPresent(), is(true));
        assertThat(messages.get(topic1).getError().get(), instanceOf(PulsarClientException.class));
        assertThat(messages.get(topic2).getError().isPresent(), is(false));
        assertThat(messages.get(topic2).getMessages(), hasSize(1));
        assertThat(messages.get(topic2).getMessages().get(0).getKey(), is("blah"));
        assertThat(new String(messages.get(topic2).getMessages().get(0).getData(), UTF_8), is("foobar"));
    }

    @Test
    public void testFetchBatch() throws Exception {
        Fetcher f = new FetcherImpl(pulsarClient, timer);

        TopicPartition topic = new TopicPartition("fetch-batch", 0);

        // timeout is infinite, fetch returns when we have any bytes at all
        CompletableFuture<Map<TopicPartition, ? extends Fetcher.Result>> result =
            f.fetch(NAMESPACE, ImmutableMap.of(topic, 0L), (byteCount) -> byteCount >= 0, 0, TimeUnit.SECONDS);

        try (Producer<String> p = pulsarClient.newProducer(Schema.STRING).topic(NAMESPACE + "/" + topic.topic())
                .batchingMaxMessages(100).batchingMaxPublishDelay(1, TimeUnit.HOURS).create()) {
            p.newMessage().key("blah0").value("foobar0").sendAsync();
            p.newMessage().key("blah1").value("foobar1").sendAsync();
            p.flush();
        }

        Map<TopicPartition, ? extends Fetcher.Result> messages = result.get();
        assertThat(messages.keySet(), contains(topic));
        assertThat(messages.get(topic).getError().isPresent(), is(false));
        assertThat(messages.get(topic).getMessages(), hasSize(1));
        assertThat(messages.get(topic).getMessages().get(0).getKey(), is("blah0"));
        assertThat(new String(messages.get(topic).getMessages().get(0).getData(), UTF_8), is("foobar0"));

        long newOffset = MessageIdUtils.getOffset(messages.get(topic).getMessages().get(0).getMessageId()) + 1;
        result = f.fetch(NAMESPACE, ImmutableMap.of(topic, newOffset), (byteCount) -> byteCount >= 0, 0, TimeUnit.SECONDS);

        messages = result.get();
        assertThat(messages.keySet(), contains(topic));
        assertThat(messages.get(topic).getError().isPresent(), is(false));
        assertThat(messages.get(topic).getMessages(), hasSize(1));
        assertThat(messages.get(topic).getMessages().get(0).getKey(), is("blah1"));
        assertThat(new String(messages.get(topic).getMessages().get(0).getData(), UTF_8), is("foobar1"));
    }

}
