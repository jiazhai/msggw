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

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;

import org.junit.Test;

public class HandlerAuthTest extends HandlerTestBase {

    @Test(timeout=30000, expected=ExecutionException.class)
    public void testBadMechanism() throws Exception {
        Properties props = producerProperties();
        props.put("sasl.jaas.config",
                  "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule"
                  + " required unsecuredLoginStringClaim_sub=\"alice\";");
        props.put("sasl.mechanism", "OAUTHBEARER");

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            producer.send(new ProducerRecord<>("blah", "key1", "value1")).get();
        }
    }

    @Test(timeout=30000, expected=ExecutionException.class)
    public void testBadToken() throws Exception {
        Properties props = producerProperties();
        props.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required"
                  + " username=\"public/user1\" password=\"blahlbahblah\";");

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            producer.send(new ProducerRecord<>("blah", "key1", "value1")).get();
        }
    }

    @Test(timeout=30000, expected=ExecutionException.class)
    public void testNonExistingNamespace() throws Exception {
        Properties props = producerProperties();
        props.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required"
                  + " username=\"public/doesntexist\" password=\"" + pulsar.USER1_TOKEN + "\";");

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            producer.send(new ProducerRecord<>("blah", "key1", "value1")).get();
        }
    }

    @Test(timeout=30000, expected=ExecutionException.class)
    public void testUnauthorizedNamespace() throws Exception {
        Properties props = producerProperties();
        props.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required"
                  + " username=\"public/user2\" password=\"" + pulsar.USER1_TOKEN + "\";");

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            producer.send(new ProducerRecord<>("blah", "key1", "value1")).get();
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
}
