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

import com.google.common.base.Ticker;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import io.netty.util.Timer;
import io.netty.util.HashedWheelTimer;
import io.streaml.msggw.MockPulsarCluster;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.Phaser;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.Ignore;

public class HandlerMultiClientConsumerGroupTest extends HandlerTestBase {

    @Test(timeout=1000000)
    public void testMultiClientConsumerGroup() throws Exception {
        final int numConsumers = 10;
        final int numProducers = 10;
        Properties props = consumerProperties();
        props.setProperty("group.id", "testgroup");
        List<String> topics = new ArrayList<>();
        Map<String, Producer<String>> producers = new HashMap<>();

        log.info("Creating {} pulsar producers", numProducers);
        for (int i = 0; i < numProducers; i++) {
            String topic = UUID.randomUUID().toString();
            topics.add(topic);
            producers.put(topic,
                          pulsarClient.newProducer(Schema.STRING).topic("public/user1/" + topic).create());
        }

        log.info("Create {} kafka consumers", numConsumers);
        Set<String> messages = ConcurrentHashMap.newKeySet();
        ExecutorService executor = Executors.newFixedThreadPool(numConsumers,
                new ThreadFactoryBuilder().setNameFormat("test-consumer-group").setDaemon(true)
                .setUncaughtExceptionHandler((t, e) -> {
                        log.error("Caught exception in thread {}", t, e);
                }).build());
        CompletableFuture<Void> errorFuture = new CompletableFuture<>();
        Set<KafkaConsumer> consumersWithPartitions = ConcurrentHashMap.newKeySet();
        for (int i = 0; i < numConsumers; i++) {
            executor.submit(() -> {
                    try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
                        consumer.subscribe(topics, new ConsumerRebalanceListener() {
                                @Override
                                public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                                }

                                @Override
                                public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                                    if (!partitions.isEmpty()) {
                                        consumersWithPartitions.add(consumer);
                                    }
                                }
                            });
                        boolean done = false;
                        while (!done) {
                            ConsumerRecords<String, String> records = consumer.poll(5000);
                            for (ConsumerRecord<String, String> r : records) {
                                log.info("Got message for {} on {}", r.value(), consumer);

                                boolean ret = messages.add(r.value());
                                if (messages.size() >= topics.size()) {
                                    done = true;
                                }
                            }
                        }
                    } catch (Throwable t) {
                        log.error("Error in consumer", t);
                        errorFuture.completeExceptionally(t);
                    }
                });
        }

        log.info("Wait for more than one consumer to get partitions");
        while (consumersWithPartitions.size() < 2 && !errorFuture.isDone()) {
            Thread.sleep(100);
        }
        if (errorFuture.isDone()) {
            errorFuture.join();
        }

        log.info("Send 1 message on each topic");
        for (Map.Entry<String, Producer<String>> e : producers.entrySet()) {
            e.getValue().newMessage().value(e.getKey()).sendAsync();
        }

        log.info("Each message should be received on some consumer");
        while (messages.size() < topics.size() && !errorFuture.isDone()) {
            log.info("Received {} messages {}", messages.size(), messages);
            Thread.sleep(1000);
        }
        if (errorFuture.isDone()) {
            errorFuture.join();
        }

        log.info("Success! {} messages received", messages.size());

        log.info("Shutting down executor");
        executor.shutdownNow();
        executor.awaitTermination(10, TimeUnit.SECONDS);
        log.info("Done");
    }
}
