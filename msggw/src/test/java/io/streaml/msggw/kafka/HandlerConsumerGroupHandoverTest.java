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
import static org.mockito.Mockito.*;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;

import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HandlerConsumerGroupHandoverTest extends HandlerTestBase {
    private static final Logger log = LoggerFactory.getLogger(HandlerConsumerGroupHandoverTest.class);

    @Test
    public void testConsumerGroupHandover() throws Exception {
        try (Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .topic("handover").create()) {
            // precreate the topic to work around https://github.com/apache/pulsar/issues/5259
        }

        Properties props = consumerProperties();
        props.setProperty("group.id", "testhandover0");

        ExecutorService executor = Executors.newSingleThreadExecutor(
                new ThreadFactoryBuilder().setNameFormat("test-consumer-handover-%d").setDaemon(true)
                .setUncaughtExceptionHandler((t, e) -> {
                        log.error("Caught exception in thread {}", t, e);
                }).build());
        verify(storage, times(0)).read(anyString());
        AtomicBoolean runConsumer = new AtomicBoolean(true);
        LinkedBlockingQueue<String> recvQ = new LinkedBlockingQueue<>();

        executor.submit(() -> {
                try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
                    consumer.subscribe(Lists.newArrayList("handover"));

                    while (runConsumer.get()) {
                        ConsumerRecords<String, String> records = consumer.poll(500);
                        for (ConsumerRecord<String, String> r : records) {
                            log.info("Received record {}", r.value());
                            recvQ.put(r.value());
                        }
                    }
                }
                return null;
            });

        try (Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .topic("public/user1/handover").create()) {
            producer.send("foobar");
            assertThat(recvQ.poll(10, TimeUnit.SECONDS), equalTo("foobar"));
            verify(storage, times(1)).read(anyString());
            // once to set generation, onces to set assignments
            verify(storage.groups.get(ConsumerGroups.groupId("public/user1", "testhandover0")), times(2))
                .writeAssignment(anyObject(), anyLong());
            restartHandler();

            // wait for the group to get to stable state
            for (int i = 0; i < 10; i++) {
                if (groups.getCurrentState("public/user1", "testhandover0")
                    instanceof ConsumerGroupsImpl.StableState) {
                    break;
                }
                Thread.sleep(1000);
            }
            producer.send("barfoo");
            assertThat(recvQ.poll(10, TimeUnit.SECONDS), equalTo("barfoo"));
            verify(storage, times(2)).read(anyString());
            verify(storage.groups.get(ConsumerGroups.groupId("public/user1", "testhandover0")), times(2))
                .writeAssignment(anyObject(), anyLong());
        }
    }
}
