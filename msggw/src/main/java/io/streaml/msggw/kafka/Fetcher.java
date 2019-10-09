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

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import org.apache.kafka.common.TopicPartition;

import org.apache.pulsar.client.api.Message;

/**
 * Interface for concurrently fetching a bunch of messages from a bunch of topics.
 */
public interface Fetcher {

    /**
     * Fetch messages from a set of topics starting at given offsets.
     * @param namespace The pulsar namespace that the topics live in
     * @param topicsAndOffsets A list of topics and the offsets to fetch from at each topic
     * @param hasEnoughBytes A predicate used to decide whether enough data has been fetched
     * @param timeout The maximum time the fetch operation can take. A value less than or equal to 1 means infinite
     * @param timeoutUnit The unit for the timeout
     * @return A future which, when completed, yields a map from topic to result
     */
    CompletableFuture<Map<TopicPartition, ? extends Result>> fetch(String namespace,
                                                                   Map<TopicPartition, Long> topicsAndOffsets,
                                                                   Predicate<Integer> haveEnoughBytes,
                                                                   long timeout,
                                                                   TimeUnit timeoutUnit);

    /**
     * Result of a fetch for a single topic.
     */
    interface Result {
        /**
         * @return Optional error. Empty if no exception occurred for topic
         */
        Optional<Throwable> getError();

        /**
         * @return A list of messages fetched for the topic
         */
        List<Message<byte[]>> getMessages();
    }
}

