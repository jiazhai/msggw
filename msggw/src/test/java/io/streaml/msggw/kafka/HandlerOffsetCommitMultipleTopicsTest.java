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

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.Phaser;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;

import org.junit.Assert;
import org.junit.Test;

public class HandlerOffsetCommitMultipleTopicsTest extends HandlerTestBase {

    @Test(timeout=120000)
    public void testOffsetCommitMultipleTopics() throws Exception {
        int numTopics = 30;
        int numConsumers = 10;
        assertThat(numTopics, greaterThanOrEqualTo(numConsumers));
        int i = 0;

        Set<String> messagesSendAndReceived = new HashSet<>();
        Properties props = consumerProperties();
        props.setProperty("group.id", "testgroup0");

        List<String> topics = IntStream.range(0, numTopics).mapToObj(j -> new String("topic" + j))
            .collect(Collectors.toList());
        log.info("Creating pulsar producers");
        List<Producer<String>> producers = topics.stream()
            .map(t -> {
                    try {
                        return pulsarClient.newProducer(Schema.STRING).topic(t).create();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                })
            .collect(Collectors.toList());

        Set<String> topicsAssignedAfterUnsubscribe = ConcurrentHashMap.newKeySet();
        Set<KafkaConsumer<String, String>> consumersWithAssignments = ConcurrentHashMap.newKeySet();
        ExecutorService executor = Executors.newFixedThreadPool(numConsumers,
                new ThreadFactoryBuilder().setNameFormat("test-offsets-%d").setDaemon(true)
                .setUncaughtExceptionHandler((t, e) -> {
                        log.error("Caught exception in thread {}", t, e);
                }).build());
        Phaser phaser = new Phaser(1);

        log.info("Creating consumer threads");
        List<Future<?>> futures = IntStream.range(0, numConsumers)
            .mapToObj(j -> {
                    phaser.register();
                    return executor.submit(() -> {
                            try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
                                consumer.subscribe(topics);

                                while (consumer.assignment().isEmpty()
                                       || consumersWithAssignments.size() < numConsumers) {
                                    consumer.poll(100);

                                    if (!consumer.assignment().isEmpty()) {
                                        consumersWithAssignments.add(consumer);
                                    }
                                }
                                log.info("{} assigned to {}", consumer.assignment(), consumer);

                                log.info("Notify that we have assignment [phase:{}]",
                                         phaser.arriveAndAwaitAdvance()); // phase 1

                                log.info("Wait for producer to send messages [phase:{}]",
                                         phaser.arriveAndAwaitAdvance()); // phase 2

                                log.info("Start polling for messages");
                                while (!messagesSendAndReceived.isEmpty()) {
                                    ConsumerRecords<String, String> records = consumer.poll(500);
                                    for (ConsumerRecord r : records) {
                                        log.info("Received record {}", r.value());
                                        Assert.assertTrue(messagesSendAndReceived.remove(r.value()));
                                    }
                                }

                                log.info("Notify all messages received {} [phase:{}]",
                                         messagesSendAndReceived.size(),
                                         phaser.arriveAndAwaitAdvance()); // phase 3

                                consumer.commitSync();

                                log.info("Notify messages committed [phase:{}]",
                                         phaser.arriveAndAwaitAdvance()); // phase 4

                                if (j % 2 == 0) {
                                    consumer.unsubscribe();

                                    log.info("After unsubscribe {}", consumer.assignment());
                                    return;
                                }

                                while (!topicsAssignedAfterUnsubscribe.containsAll(topics)) {
                                    ConsumerRecords<String, String> records = consumer.poll(500);
                                    assertThat(records.count(), equalTo(0));

                                    topicsAssignedAfterUnsubscribe.addAll(
                                            consumer.assignment().stream().map(TopicPartition::topic)
                                            .collect(Collectors.toList()));
                                }
                                log.info("Notify half of consumers unsubscribed, topics reassigned [phase:{}]",
                                         phaser.arriveAndAwaitAdvance()); // phase 5
                                log.info("{} assigned to {}", consumer.assignment(), consumer);

                                log.info("Wait for more messages to be written [phase:{}]",
                                        phaser.arriveAndAwaitAdvance()); // phase 6

                                log.info("Start polling for remaining messages");
                                while (!messagesSendAndReceived.isEmpty()) {
                                    ConsumerRecords<String, String> records = consumer.poll(500);
                                    for (ConsumerRecord r : records) {
                                        log.info("Received record {}", r.value());
                                        Assert.assertTrue(messagesSendAndReceived.remove(r.value()));
                                    }
                                }
                                log.info("Notify all messages have been received [phase:{}]",
                                         phaser.arriveAndAwaitAdvance()); // phase 7
                            } catch (Throwable t) {
                                log.info("Caught exception", t);
                                throw t;
                            } finally {
                                log.info("Deregistering [phase:{}]", phaser.arriveAndDeregister());
                            }
                        });
                })
            .collect(Collectors.toList());

        log.info("Wait for consumers to get assignment [phase:{}]",
                 phaser.arriveAndAwaitAdvance()); // phase 1
        log.info("Consumers all started");

        log.info("Writing messages to producers");
        for (Producer<String> p : producers) {
            String value = "value" + i++;
            messagesSendAndReceived.add(value);
            p.newMessage().value(value).send();
        }

        log.info("Notify that messages have been written [phase:{}]",
                 phaser.arriveAndAwaitAdvance()); // phase 2

        log.info("Wait for consumers to receive all messages [phase:{}]",
                 phaser.arriveAndAwaitAdvance()); // phase 3

        log.info("All messages should have been received");
        assertThat(messagesSendAndReceived, empty());

        log.info("Wait for consumers to commit [phase:{}]",
                 phaser.arriveAndAwaitAdvance()); // phase 4

        log.info("Wait for half of consumers to unsubscribe [phase:{}]",
                 phaser.arriveAndAwaitAdvance()); // phase 5

        log.info("Writing more messages to producers");
        for (Producer<String> p : producers) {
            String value = "value" + i++;
            messagesSendAndReceived.add(value);
            p.newMessage().value(value).send();
        }
        log.info("Notify that messages have been written [phase:{}]",
                 phaser.arriveAndAwaitAdvance()); // phase 6

        log.info("Wait for consumers to receive all messages [phase:{}]",
                 phaser.arriveAndAwaitAdvance()); // phase 7

        for (Future<?> f : futures) {
            f.get();
        }
    }

}

