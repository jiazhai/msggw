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
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import io.streaml.msggw.kafka.proto.Kafka.ConsumerGroupAssignment;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

public interface ConsumerGroupStorage {
    public static final long NEW_VERSION = -145313;

    interface Handle {
        ConsumerGroupAssignment getAssignment() throws CorruptionException;

        CompletableFuture<Void> write(ConsumerGroupAssignment newAssignment);

        CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> getOffsetAndMetadata(
                Collection<TopicPartition> topics);
        CompletableFuture<Void> putOffsetAndMetadata(
                Map<TopicPartition, OffsetAndMetadata> offsets);

        CompletableFuture<Void> close();
    };

    class BadVersionException extends RuntimeException {
        BadVersionException(long expected, long actual) {
            super(String.format("Expected version %d but current version is %d",
                                expected, actual));
        }
        BadVersionException(String error) {
            super(error);
        }
    }

    class CorruptionException extends RuntimeException {
        CorruptionException(Exception e) {
            super(e);
        }
    }
    CompletableFuture<Handle> read(String group);
}
