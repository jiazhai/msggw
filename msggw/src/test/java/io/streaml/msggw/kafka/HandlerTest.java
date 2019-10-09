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

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

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

import org.junit.Assert;
import org.junit.Test;
import org.junit.Ignore;

public class HandlerTest extends HandlerTestBase {

    @Test(timeout=30000, expected=ExecutionException.class)
    public void testUnsupportedGetErrors() throws Exception {
        try (AdminClient admin = kafkaAdminClient()) {
            admin.describeLogDirs(Lists.newArrayList(1)).all().get();
        }
    }

    @Test(timeout=30000)
    public void testMetadataHandlerListAllTopics() throws Exception {
        // create a few topics
        try (Producer<byte[]> p1 = pulsarClient.newProducer().topic("public/user1/topic1").create();
             Producer<byte[]> p2 = pulsarClient.newProducer().topic("public/user1/topic2").create()) {
        }
        try (AdminClient admin = kafkaAdminClient()) {
            assertThat(admin.listTopics().names().get(), containsInAnyOrder("topic1", "topic2"));
        }
    }

    @Test(timeout=30000)
    public void testMetadataHandlerMetadataForSomeTopics() throws Exception {
        // create a few topics
        try (Producer<byte[]> p1 = pulsarClient.newProducer().topic("public/user1/topic1").create();
             Producer<byte[]> p2 = pulsarClient.newProducer().topic("public/user1/topic2").create();
             Producer<byte[]> p3 = pulsarClient.newProducer().topic("public/user1/topic3").create()) {
        }

        try (AdminClient admin = kafkaAdminClient()) {
            Thread.sleep(1000);
            Map<String, TopicDescription> result = admin.describeTopics(Lists.newArrayList("topic3", "topic1"))
                .all().get();
            assertThat(result.keySet(), containsInAnyOrder("topic1", "topic3"));
            assertThat(result.values().stream().map(v -> v.name()).collect(Collectors.toList()),
                       containsInAnyOrder("topic1", "topic3"));
                       Assert.assertTrue(result.values().stream().allMatch(v -> !v.isInternal()));
        }
    }

    @Test(timeout=30000)
    public void testMetadataHandlerMetadataForSomeTopicsSomeDontExistNoCreate() throws Exception {
        // create a few topics
        try (Producer<byte[]> p1 = pulsarClient.newProducer().topic("public/user1/topic1").create()) {
        }

        try (AdminClient admin = kafkaAdminClient()) {
            Map<String, KafkaFuture<TopicDescription>> result = admin.describeTopics(
                    Lists.newArrayList("topic3", "topic1")).values();
            assertThat(result.keySet(), containsInAnyOrder("topic1", "topic3"));
            assertThat(result.get("topic1").get().name(), equalTo("topic1"));
            try {
                result.get("topic3").get();
                Assert.fail("Should throw an exception");
            } catch (ExecutionException e) {
                assertThat(e.getCause(), instanceOf(UnknownTopicOrPartitionException.class));
            }
        }
    }

    @Test(timeout=30000)
    public void testMetadataHandlerMetadataForSomeTopicsSomeDontExistAutoCreate() throws Exception {
        Properties props = consumerProperties();

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            List<PartitionInfo> partitions = consumer.partitionsFor("topic1");
            assertThat(partitions, hasSize(1));
            assertThat(partitions.get(0).topic(), equalTo("topic1"));
        }
    }

    @Test(timeout=30000)
    public void testSimpleProduce() throws Exception {
        Properties props = producerProperties();

        String topic = "simple-produce";
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props);
             Consumer<byte[]> consumer = pulsarClient.newConsumer().topic("public/user1/" + topic)
                .subscriptionName("sub1").subscribe()) {
            producer.send(new ProducerRecord<>(topic, "key1", "value1")).get();
            producer.flush();

            Message<byte[]> m = consumer.receive(10, TimeUnit.SECONDS);
            assertThat(m, notNullValue());
            assertThat(new String(m.getValue(), UTF_8), equalTo("value1"));
            assertThat(new String(m.getKeyBytes(), UTF_8), equalTo("key1"));
        }
    }

    @Test(timeout=30000)
    public void testMultiProduce() throws Exception {
        Properties props = producerProperties();

        String topic = "multi-produce";
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props);
             Consumer<byte[]> consumer = pulsarClient.newConsumer().topic("public/user1/" + topic)
                .subscriptionName("sub1").subscribe()) {
            producer.send(new ProducerRecord<>(topic, "key1", "value1")).get();
            producer.send(new ProducerRecord<>(topic, "key2", "value2")).get();
            producer.send(new ProducerRecord<>(topic, "key3", "value3")).get();
            producer.flush();

            Message<byte[]> m = consumer.receive(10, TimeUnit.SECONDS);
            assertThat(m, notNullValue());
            assertThat(new String(m.getValue(), UTF_8), equalTo("value1"));
            assertThat(new String(m.getKeyBytes(), UTF_8), equalTo("key1"));

            m = consumer.receive(10, TimeUnit.SECONDS);
            assertThat(m, notNullValue());
            assertThat(new String(m.getValue(), UTF_8), equalTo("value2"));
            assertThat(new String(m.getKeyBytes(), UTF_8), equalTo("key2"));

            m = consumer.receive(10, TimeUnit.SECONDS);
            assertThat(m, notNullValue());
            assertThat(new String(m.getValue(), UTF_8), equalTo("value3"));
            assertThat(new String(m.getKeyBytes(), UTF_8), equalTo("key3"));
        }
    }

    @Test(timeout=30000)
    public void testMultiProduceMultiTopic() throws Exception {
        Properties props = producerProperties();

        String topic1 = "multi-topic1";
        String topic2 = "multi-topic2";
        String topic3 = "multi-topic3";
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props);
             Consumer<byte[]> consumer1 = pulsarClient.newConsumer()
                .topic("public/user1/" + topic1)
                .subscriptionName("sub1").subscribe();
             Consumer<byte[]> consumer2 = pulsarClient.newConsumer()
                .topic("public/user1/" + topic2)
                .subscriptionName("sub1").subscribe();
             Consumer<byte[]> consumer3 = pulsarClient.newConsumer()
                .topic("public/user1/" + topic3)
                .subscriptionName("sub1").subscribe()) {
            producer.send(new ProducerRecord<>(topic1, "key1", "value1")).get();
            producer.send(new ProducerRecord<>(topic2, "key2", "value2")).get();
            producer.send(new ProducerRecord<>(topic3, "key3", "value3")).get();
            producer.flush();

            Message<byte[]> m = consumer1.receive(10, TimeUnit.SECONDS);
            assertThat(m, notNullValue());
            assertThat(new String(m.getValue(), UTF_8), equalTo("value1"));
            assertThat(new String(m.getKeyBytes(), UTF_8), equalTo("key1"));

            m = consumer2.receive(10, TimeUnit.SECONDS);
            assertThat(m, notNullValue());
            assertThat(new String(m.getValue(), UTF_8), equalTo("value2"));
            assertThat(new String(m.getKeyBytes(), UTF_8), equalTo("key2"));

            m = consumer3.receive(10, TimeUnit.SECONDS);
            assertThat(m, notNullValue());
            assertThat(new String(m.getValue(), UTF_8), equalTo("value3"));
            assertThat(new String(m.getKeyBytes(), UTF_8), equalTo("key3"));
        }
    }

    @Test(timeout=30000)
    public void testSimpleFetch() throws Exception {
        Properties props = consumerProperties();

        String topic = "simple-fetch";
        log.info("Creating pulsar producer and kafka consumer");
        try (Producer<String> p = pulsarClient.newProducer(Schema.STRING)
                .topic("public/user1/" + topic).create();
             KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            p.newMessage().key("blah").value("foobar").send();

            log.info("Assigning topic to consumer");
            TopicPartition tp = new TopicPartition(topic, 0);
            consumer.assign(Arrays.asList(tp));

            log.info("Polling consumer for records");
            ConsumerRecords<String, String> records = consumer.poll(50000);
            assertThat(records.count(), equalTo(1));
            assertThat(records.records(tp).size(), equalTo(1));
            assertThat(records.records(tp).get(0).key(), equalTo("blah"));
            assertThat(records.records(tp).get(0).value(), equalTo("foobar"));

            p.newMessage().key("blah2").value("foobar2").send();

            log.info("Polling consumer for records again");
            records = consumer.poll(5000);
            assertThat(records.count(), equalTo(1));
            assertThat(records.records(tp).size(), equalTo(1));
            assertThat(records.records(tp).get(0).key(), equalTo("blah2"));
            assertThat(records.records(tp).get(0).value(), equalTo("foobar2"));

            p.newMessage().key("blah3").value("foobar3").send();
            p.newMessage().key("blah4").value("foobar4").send();

            records = consumer.poll(5000);
            assertThat(records.count(), equalTo(2));
            assertThat(records.records(tp).size(), equalTo(2));
            assertThat(records.records(tp).get(0).key(), equalTo("blah3"));
            assertThat(records.records(tp).get(0).value(), equalTo("foobar3"));
            assertThat(records.records(tp).get(1).key(), equalTo("blah4"));
            assertThat(records.records(tp).get(1).value(), equalTo("foobar4"));
            consumer.commitSync();
        }
    }

    @Test(timeout=30000)
    public void testSingleClientConsumerGroup() throws Exception {
        Properties props = consumerProperties();
        props.setProperty("group.id", "testgroup");

        String topic = "single-consumer";
        log.info("Creating pulsar producer and kafka consumer");
        try (Producer<String> p = pulsarClient.newProducer(Schema.STRING)
                .topic("public/user1/" + topic).create();
             KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            p.newMessage().key("blah").value("foobar").send();

            log.info("Subscribe consumer to topic");
            consumer.subscribe(Lists.newArrayList(topic));

            log.info("Polling consumer for records");
            ConsumerRecords<String, String> records = consumer.poll(50000);
            assertThat(records.count(), equalTo(1));
            for (ConsumerRecord r : records) {
                assertThat(r.topic(), equalTo(topic));
                assertThat(r.key(), equalTo("blah"));
                assertThat(r.value(), equalTo("foobar"));
            }
            p.newMessage().key("blah2").value("foobar2").send();
        }
    }
}
