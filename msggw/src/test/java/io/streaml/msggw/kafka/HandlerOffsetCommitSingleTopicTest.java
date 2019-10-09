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

import com.google.common.collect.Lists;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;

import org.junit.Test;

public class HandlerOffsetCommitSingleTopicTest extends HandlerTestBase {

    @Test(timeout=30000)
    public void testOffsetCommitSingleTopic() throws Exception {
        Properties props = consumerProperties();
        props.setProperty("group.id", "testgroup");

        String topic = "single-consumer";
        log.info("Creating pulsar producer & kafka consumer");
        try (Producer<String> p = pulsarClient.newProducer(Schema.STRING).topic("public/user1/" + topic).create();
             KafkaConsumer<String, String> consumer1 = new KafkaConsumer<>(props)) {
            p.newMessage().key("blah").value("foobar").send();

            log.info("Subscribe consumer to topic");
            consumer1.subscribe(Lists.newArrayList(topic));

            log.info("Polling consumer for records");
            ConsumerRecords<String, String> records1 = consumer1.poll(50000);
            assertThat(records1.count(), equalTo(1));
            for (ConsumerRecord r : records1) {
                assertThat(r.topic(), equalTo(topic));
                assertThat(r.key(), equalTo("blah"));
                assertThat(r.value(), equalTo("foobar"));
            }
            consumer1.commitSync();

            log.info("Create second kakfa consumer, offset should be preserved");
            KafkaConsumer<String, String> consumer2 = new KafkaConsumer<>(props);

            log.info("Subscribe consumer to topic");
            consumer2.subscribe(Lists.newArrayList(topic));

            log.info("Unsubscribe previous consumer");
            consumer1.unsubscribe();
            p.newMessage().key("foobar").value("blah").send();

            log.info("Polling consumer for records");
            ConsumerRecords<String, String> records2 = consumer2.poll(50000);
            assertThat(records2.count(), equalTo(1));
            for (ConsumerRecord r : records2) {
                assertThat(r.topic(), equalTo(topic));
                assertThat(r.key(), equalTo("foobar"));
                assertThat(r.value(), equalTo("blah"));
            }
            consumer2.close();
        }
    }
}
