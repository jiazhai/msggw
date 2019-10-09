package io.streaml.msggw.http;

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

import io.prometheus.client.Counter;
import io.prometheus.client.Histogram;
import io.prometheus.client.SimpleCollector;
import org.apache.pulsar.common.naming.TopicName;

class Metrics {
    private static final String[] labelNames = new String[] { "method", "tenant", "namespace", "topic" };

    private static String[] toLabels(String method, TopicName topic) {
        return new String[] { method, topic.getTenant(), topic.getNamespace(), topic.toString() };
    }

    static final Counter ops = Counter
        .build("msggw_restdata_ops", "Counter of messages posted")
        .labelNames(labelNames)
        .create()
        .register();

    static final Counter bytes = Counter
        .build("msggw_restdata_bytes", "Counter of bytes in messages posted")
        .labelNames(labelNames)
        .create()
        .register();

    static final Counter errors = Counter
        .build("msggw_restdata_errors", "Counter of errors for messages posted")
        .labelNames(labelNames)
        .create()
        .register();

    static final Histogram opLatency = Histogram
        .build("msggw_restdata_oplatency", "Latency of operation")
        .labelNames(labelNames)
        .create()
        .register();

    static Histogram.Timer registerOpStart(String method, TopicName topic) {
        ops.labels(toLabels(method, topic)).inc();

        return opLatency.labels(toLabels(method, topic)).startTimer();
    }

    static void registerOpBytes(String method, TopicName topic, int size) {
        bytes.labels(toLabels(method, topic)).inc(size);
    }

    static void registerOpError(String method, TopicName topic) {
        errors.labels(toLabels(method, topic)).inc();
    }
}

