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

import static org.mockito.Mockito.spy;

import com.google.common.collect.ImmutableMap;
import java.util.Collection;
import java.util.Map;
import java.util.function.Function;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import io.streaml.msggw.kafka.proto.Kafka.ConsumerGroupAssignment;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MockConsumerGroupStorage implements ConsumerGroupStorage {
    private final static Logger log = LoggerFactory.getLogger(MockConsumerGroupStorage.class);
    final ConcurrentHashMap<String, GroupData> groups = new ConcurrentHashMap<>();

    @Override
    public CompletableFuture<Handle> read(String groupName) {
        return CompletableFuture.completedFuture(
                new MockHandle(groups.compute(groupName, (key, value) -> {
                            if (value != null) {
                                return value;
                            } else {
                                return spy(new GroupData());
                            }
                        })));
    }

    public static class GroupData {
        private long version = NEW_VERSION;
        private ConsumerGroupAssignment assignment = null;
        private final Map<TopicPartition, OffsetAndMetadata> offsets = new ConcurrentHashMap<>();

        synchronized ConsumerGroupAssignment getAssignment() {
            return assignment;
        }

        synchronized long getVersion() {
            return version;
        }

        synchronized long writeAssignment(ConsumerGroupAssignment assignment, long expectedVersion)
                throws BadVersionException {
            if (version == expectedVersion) {
                this.assignment = assignment;
                return ++version;
            } else {
                throw new BadVersionException(expectedVersion, version);
            }
        }

        synchronized Map<TopicPartition, OffsetAndMetadata> getOffsetAndMetadata(Collection<TopicPartition> topics) {
            return topics.stream().filter(offsets::containsKey)
                .collect(ImmutableMap.toImmutableMap(Function.identity(),
                                                     offsets::get));
        }

        synchronized void putOffsetAndMetadata(Map<TopicPartition, OffsetAndMetadata> offsets) {
            this.offsets.putAll(offsets);
        }
    }

    private static class MockHandle implements ConsumerGroupStorage.Handle {
        private final GroupData data;
        private long currentVersion;

        MockHandle(GroupData data) {
            this.data = data;
            this.currentVersion = data.getVersion();
        }

        @Override
        public ConsumerGroupAssignment getAssignment() {
            return data.getAssignment();
        }

        @Override
        public CompletableFuture<Void> write(ConsumerGroupAssignment newAssignment) {
            CompletableFuture<Void> promise = new CompletableFuture<>();
            synchronized (this) {
                try {
                    currentVersion = data.writeAssignment(newAssignment, currentVersion);
                    promise.complete(null);
                } catch (Exception e) {
                    promise.completeExceptionally(e);
                }
            }
            return promise;
        }

        @Override
        public CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> getOffsetAndMetadata(
                Collection<TopicPartition> topics) {
            return CompletableFuture.completedFuture(data.getOffsetAndMetadata(topics));
        }

        @Override
        public CompletableFuture<Void> putOffsetAndMetadata(
                Map<TopicPartition, OffsetAndMetadata> offsets) {
            data.putOffsetAndMetadata(offsets);
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletableFuture<Void> close() {
            return CompletableFuture.completedFuture(null);
        }
    }
}
