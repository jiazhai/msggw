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

import io.streaml.mltable.MLTable;
import io.streaml.msggw.kafka.proto.Kafka.ConsumerGroupAssignment;
import io.streaml.msggw.kafka.proto.Kafka.OffsetAndMetadataFormat;
import org.apache.bookkeeper.client.api.BookKeeper;
import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public class MLTableConsumerGroupStorage implements ConsumerGroupStorage {
    private final static Logger log = LoggerFactory.getLogger(MLTableConsumerGroupStorage.class);
    private final static String CONSUMER_GROUP_KEY = "ConsumerGroupAssignments";
    private final static String OFFSET_KEY_TEMPLATE = "offset-%s";
    private final ManagedLedgerFactory mlFactory;
    private final BookKeeper bookkeeper;
    private final ExecutorService executor;
    private final int ensembleSize;
    private final int writeQuorumSize;
    private final int ackQuorumSize;

    public MLTableConsumerGroupStorage(ManagedLedgerFactory mlFactory,
                                       BookKeeper bookkeeper,
                                       ExecutorService executor,
                                       int ensembleSize,
                                       int writeQuorumSize,
                                       int ackQuorumSize) {
        this.mlFactory = mlFactory;
        this.bookkeeper = bookkeeper;
        this.executor = executor;
        this.ensembleSize = ensembleSize;
        this.writeQuorumSize = writeQuorumSize;
        this.ackQuorumSize = ackQuorumSize;
    }

    @Override
    public CompletableFuture<Handle> read(String group) {
        return MLTable.newBuilder()
                .withTableName("__consumer_group." + group)
                .withExecutor(executor)
                .withManagedLedgerFactory(mlFactory)
                .withBookKeeperClient(bookkeeper)
                .withEnsembleSize(ensembleSize)
                .withWriteQuorumSize(writeQuorumSize)
                .withAckQuorumSize(ackQuorumSize)
                .build()
                .thenComposeAsync(mltable ->
                                mltable.get(CONSUMER_GROUP_KEY)
                                        .thenApplyAsync(value -> new MLTableHandle(mltable, executor, value),
                                                executor),
                        executor);
    }

    private static class MLTableHandle implements Handle {
        private final MLTable table;
        private final ExecutorService executor;
        private final AtomicReference<MLTable.Value> value;

        MLTableHandle(MLTable table, ExecutorService executor,
                      MLTable.Value value) {
            this.table = table;
            this.executor = executor;
            this.value = new AtomicReference<>(value);
        }

        @Override
        public ConsumerGroupAssignment getAssignment() throws CorruptionException {
            if (value.get() == null) {
                return null;
            }
            try {
                return ConsumerGroupAssignment.newBuilder().mergeFrom(value.get().value()).build();
            } catch (Exception e) {
                throw new CorruptionException(e);
            }
        }

        @Override
        public CompletableFuture<Void> write(ConsumerGroupAssignment newAssignment) {
            MLTable.Value current = value.get();
            byte[] bytes = newAssignment.toByteArray();
            CompletableFuture<Void> promise = new CompletableFuture<>();
            table.put(CONSUMER_GROUP_KEY, bytes, current == null ? MLTable.NEW : current.version())
                .whenCompleteAsync((newVersion, exception) -> {
                        if (exception != null) {
                            promise.completeExceptionally(exception);
                        } else {
                            MLTable.Value newValue = new MLTable.Value() {
                                    @Override
                                    public byte[] value() {
                                        return bytes;
                                    }
                                    @Override
                                    public long version() {
                                        return newVersion;
                                    }
                                };
                            if (value.compareAndSet(current, newValue)) {
                                promise.complete(null);
                            } else {
                                promise.completeExceptionally(
                                        new BadVersionException("local CAS failed"));
                            }
                        }
                    }, executor);
            return promise;
        }

        @Override
        public CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> getOffsetAndMetadata(
                Collection<TopicPartition> topics) {
            ConcurrentHashMap<TopicPartition, OffsetAndMetadata> offsets
                = new ConcurrentHashMap<>();
            List<CompletableFuture<?>> futures = topics.stream().map((t) -> {
                    String key = String.format(OFFSET_KEY_TEMPLATE, t.toString());
                    return table.get(key)
                        .thenApply((value) -> {
                                if (value == null) {
                                    return null;
                                }
                                try {
                                    OffsetAndMetadataFormat format = OffsetAndMetadataFormat.newBuilder()
                                        .mergeFrom(value.value()).build();
                                    offsets.put(t, new OffsetAndMetadata(format.getOffset(),
                                                                     format.getMetadata()));
                                    return null;
                                } catch (Exception e) {
                                    throw new CorruptionException(e);
                                }
                            });
                }).collect(Collectors.toList());
            return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                .thenApply(ignore -> offsets);
        }

        @Override
        public CompletableFuture<Void> putOffsetAndMetadata(
                Map<TopicPartition, OffsetAndMetadata> offsets) {
            List<CompletableFuture<?>> futures = offsets.entrySet().stream()
                .map(e -> {
                        String key = String.format(OFFSET_KEY_TEMPLATE, e.getKey().toString());
                        byte[] bytes = OffsetAndMetadataFormat.newBuilder()
                            .setOffset(e.getValue().offset())
                            .setMetadata(e.getValue().metadata())
                            .build().toByteArray();
                        return table.get(key).thenCompose(
                                value -> {
                                    if (value != null) {
                                        return table.put(key, bytes, value.version());
                                    } else {
                                        return table.put(key, bytes, MLTable.NEW);
                                    }
                                });
                    })
                .collect(Collectors.toList());
            return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
        }

        @Override
        public CompletableFuture<Void> close() {
            return table.closeAsync();
        }
    };
}
