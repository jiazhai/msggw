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

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import io.streaml.msggw.kafka.proto.Kafka.ConsumerGroupAssignment;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

public class NullConsumerGroupStorage implements ConsumerGroupStorage {

    @Override
    public CompletableFuture<ConsumerGroupStorage.Handle> read(String groupName) {
        return CompletableFuture.completedFuture(
                new ConsumerGroupStorage.Handle() {
                    @Override
                    public ConsumerGroupAssignment getAssignment() {
                        return null;
                    }

                    @Override
                    public CompletableFuture<Void> write(ConsumerGroupAssignment newAssignment) {
                        return CompletableFuture.completedFuture(null);
                    }

                    @Override
                    public CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> getOffsetAndMetadata(
                            Collection<TopicPartition> topics) {
                        return CompletableFuture.completedFuture(Collections.emptyMap());
                    }
                    @Override
                    public CompletableFuture<Void> putOffsetAndMetadata(
                            Map<TopicPartition, OffsetAndMetadata> offsets) {
                        return CompletableFuture.completedFuture(null);
                    }
                    @Override
                    public CompletableFuture<Void> close() {
                        return CompletableFuture.completedFuture(null);
                    }
                });
    }
}
