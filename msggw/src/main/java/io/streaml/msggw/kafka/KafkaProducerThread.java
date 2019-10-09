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

import com.github.benmanes.caffeine.cache.Ticker;

import io.streaml.msggw.AbstractProducerThread;
import io.streaml.msggw.AbstractProducerThread.MessageEvent;

import java.util.Base64;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.record.Record;

import org.apache.pulsar.client.api.PulsarClient;

public class KafkaProducerThread extends AbstractProducerThread<Record> {

    public KafkaProducerThread(PulsarClient pulsarClient) {
        super(pulsarClient, "kafka-producer-thread", 1024, 1024,
              Ticker.systemTicker(), 128, 300);
    }

    @Override
    public void translateMessage(Record message, MessageEvent event) {
        if (message.hasKey()) {
            byte[] key = new byte[message.keySize()];
            message.key().get(key);
            event.keyBytes = key;
        }
        if (message.hasValue()) {
            byte[] value = new byte[message.valueSize()];
            message.value().get(value);
            event.value = value;
        }
        if (message.sequence() >= 0) {
            event.sequenceId = message.sequence();
        }
        if (message.timestamp() >= 0) {
            event.eventTime = message.timestamp();
        }
        for (Header h : message.headers()) {
            event.properties.put(h.key(), Base64.getEncoder().encodeToString(h.value()));
        }
    }
}
