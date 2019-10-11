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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import io.streaml.mltable.MockBKCluster;
import io.streaml.mltable.MockBKClient;
import io.streaml.msggw.kafka.proto.Kafka.ConsumerGroupAssignment;

import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.junit.Test;
import org.junit.Before;
import org.junit.After;

public class MLTableConsumerGroupStorageTest {
    private static final Logger log = LoggerFactory.getLogger(MLTableConsumerGroupStorageTest.class);
    private MockBKCluster cluster;
    private MockBKClient bkc;
    private ExecutorService executor;

    @Before
    public void setup() throws Exception {
        cluster = new MockBKCluster();
        cluster.start();

        executor = Executors.newSingleThreadExecutor(
                new ThreadFactoryBuilder().setNameFormat("test-").setUncaughtExceptionHandler(
                        (t, e) -> {
                            log.error("Caught exception in thread {}", t, e);
                            }).build());
        bkc = new MockBKClient(executor);
    }

    @After
    public void teardown() throws Exception {
        executor.shutdownNow();
        if (cluster != null) {
            cluster.stop();
        }
    }

    @Test(timeout=30000)
    public void testConsumerGroupAssignment() throws Exception {
        MLTableConsumerGroupStorage storage = new MLTableConsumerGroupStorage(
                cluster.getFactory(), bkc, executor, 1, 1, 1);

        log.info("Read from storage, should be empty");
        ConsumerGroupStorage.Handle handle1 = storage.read("my-group").join();
        assertThat(handle1.getAssignment(), nullValue());

        log.info("Write an assignment to storage");
        ConsumerGroupAssignment assign1 =
            ConsumerGroupAssignment.newBuilder().setGeneration(1).build();
        handle1.write(assign1).join();
        handle1.close().join();

        log.info("Reading back, it shouldn't be null");
        ConsumerGroupStorage.Handle handle2 = storage.read("my-group").join();
        assertThat(handle2.getAssignment(), not(nullValue()));
        assertThat(handle2.getAssignment().getGeneration(), equalTo(1));

        log.info("Write something with new handle");
        handle2.write(assign1.toBuilder().setGeneration(2).build()).join();
        assertThat(handle2.getAssignment().getGeneration(), equalTo(2));

        log.info("Write again with new handle");
        handle2.write(assign1.toBuilder().setGeneration(3).build()).join();
        assertThat(handle2.getAssignment().getGeneration(), equalTo(3));
        handle2.close().join();

        log.info("Reading back one more time, it shouldn't be null");
        ConsumerGroupStorage.Handle handle3 = storage.read("my-group").join();
        assertThat(handle3.getAssignment(), not(nullValue()));
        assertThat(handle3.getAssignment().getGeneration(), equalTo(3));
        handle3.close().join();
    }

    @Test(timeout=30000)
    public void testOffsets() {
        MLTableConsumerGroupStorage storage = new MLTableConsumerGroupStorage(
                cluster.getFactory(), bkc, executor, 1, 1, 1);

        log.info("Read in handle");
        ConsumerGroupStorage.Handle handle1 = storage.read("").join();
        assertThat(handle1.getAssignment(), nullValue());

        log.info("Write some offsets");
        handle1.putOffsetAndMetadata(ImmutableMap.of(new TopicPartition("foobar", 0),
                                                     new OffsetAndMetadata(1234))).join();
        handle1.putOffsetAndMetadata(ImmutableMap.of(new TopicPartition("barfoo", 0),
                                                     new OffsetAndMetadata(4321))).join();

        log.info("Close handle");
        handle1.close().join(); // important for mocks

        log.info("Read back handle");
        ConsumerGroupStorage.Handle handle2 = storage.read("").join();
        Map<TopicPartition, OffsetAndMetadata> offsets1 = handle2.getOffsetAndMetadata(
                Lists.newArrayList(new TopicPartition("foobar", 0),
                                   new TopicPartition("barfoo", 0),
                                   new TopicPartition("doesnt_exist", 0))).join();

        log.info("Check offsets are as expected");
        assertThat(offsets1.entrySet(), hasSize(2));
        assertThat(offsets1, hasEntry(new TopicPartition("foobar", 0),
                                      new OffsetAndMetadata(1234)));
        assertThat(offsets1, hasEntry(new TopicPartition("barfoo", 0),
                                      new OffsetAndMetadata(4321)));

        log.info("Update an offset");
        handle2.putOffsetAndMetadata(ImmutableMap.of(new TopicPartition("barfoo", 0),
                                                     new OffsetAndMetadata(5678))).join();

        log.info("Close handle");
        handle2.close().join();

        log.info("Read back handle again");
        ConsumerGroupStorage.Handle handle3 = storage.read("").join();
        Map<TopicPartition, OffsetAndMetadata> offsets2 = handle3.getOffsetAndMetadata(
                Lists.newArrayList(new TopicPartition("foobar", 0),
                                   new TopicPartition("barfoo", 0),
                                   new TopicPartition("doesnt_exist", 0))).join();

        log.info("Check offsets are as expected");
        assertThat(offsets2.entrySet(), hasSize(2));
        assertThat(offsets2, hasEntry(new TopicPartition("foobar", 0),
                                      new OffsetAndMetadata(1234)));
        assertThat(offsets2, hasEntry(new TopicPartition("barfoo", 0),
                                      new OffsetAndMetadata(5678)));
    }
}
