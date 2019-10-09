package io.streaml.mltable;

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

import com.google.common.collect.Lists;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.EnumSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.LedgerMetadataBuilder;
import org.apache.bookkeeper.client.api.BookKeeper;
import org.apache.bookkeeper.client.api.CreateBuilder;
import org.apache.bookkeeper.client.api.CreateAdvBuilder;
import org.apache.bookkeeper.client.api.DigestType;
import org.apache.bookkeeper.client.api.OpenBuilder;
import org.apache.bookkeeper.client.api.DeleteBuilder;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.client.api.WriteFlag;
import org.apache.bookkeeper.client.api.WriteHandle;
import org.apache.bookkeeper.client.api.LastConfirmedAndEntry;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.client.api.LedgerEntry;
import org.apache.bookkeeper.client.api.LedgerEntries;
import org.apache.bookkeeper.client.impl.LedgerEntriesImpl;
import org.apache.bookkeeper.client.impl.LedgerEntryImpl;
import org.apache.bookkeeper.net.BookieSocketAddress;

public class MockBKClient implements BookKeeper {
    private final Executor executor;

    private AtomicLong idCounter = new AtomicLong(0);
    private ConcurrentHashMap<Long, List<LedgerEntry>> dataStore = new ConcurrentHashMap<>();

    public MockBKClient(Executor executor) {
        this.executor = executor;
    }

    @Override
    public CreateBuilder newCreateLedgerOp() {
        return new CreateBuilder() {
            @Override
            public CreateBuilder withEnsembleSize(int ensembleSize) { return this; }
            @Override
            public CreateBuilder withWriteQuorumSize(int writeQuorumSize) { return this; }
            @Override
            public CreateBuilder withAckQuorumSize(int ackQuorumSize) { return this; }
            @Override
            public CreateBuilder withPassword(byte[] password) { return this; }
            @Override
            public CreateBuilder withCustomMetadata(Map<String, byte[]> customMetadata) {
                return this;
            }
            @Override
            public CreateBuilder withDigestType(DigestType digestType) {
                return this;
            }
            @Override
            public CreateAdvBuilder makeAdv() { return null; }
            @Override
            public CreateBuilder withWriteFlags(EnumSet<WriteFlag> flags) { return this; }
            @Override
            public CompletableFuture<WriteHandle> execute() {
                return CompletableFuture.supplyAsync(() -> {
                        long ledgerId = idCounter.incrementAndGet();
                        LedgerMetadata metadata = LedgerMetadataBuilder.create()
                            .withEnsembleSize(1)
                            .withWriteQuorumSize(1)
                            .withAckQuorumSize(1)
                            .withPassword(new byte[0])
                            .withDigestType(DigestType.CRC32C)
                            .newEnsembleEntry(0, Lists.newArrayList(new BookieSocketAddress("127.0.0.1", 3181)))
                            .build();
                        List<LedgerEntry> ledgerData = new ArrayList<>();
                        dataStore.put(ledgerId, ledgerData);
                        return new MockWriteHandle(ledgerId, metadata, ledgerData);
                    }, executor);
            }
        };
    }

    @Override
    public OpenBuilder newOpenLedgerOp() {
        return new OpenBuilder() {
            private long ledgerId = -1;
            @Override
            public OpenBuilder withLedgerId(long ledgerId) {
                this.ledgerId = ledgerId;
                return this;
            }
            @Override
            public OpenBuilder withRecovery(boolean recovery) { return this; }
            @Override
            public OpenBuilder withPassword(byte[] password) { return this; }
            @Override
            public OpenBuilder withDigestType(DigestType digestType) { return this; }
            @Override
            public CompletableFuture<ReadHandle> execute() {
                CompletableFuture<ReadHandle> promise = new CompletableFuture<>();
                executor.execute(() -> {
                        List<LedgerEntry> entries = dataStore.get(ledgerId);
                        if (entries == null) {
                            promise.completeExceptionally(new BKException.BKNoSuchLedgerExistsException());
                            return;
                        }
                        long length = entries.stream()
                            .mapToLong(e -> e.getEntryBuffer().readableBytes())
                            .sum();
                        long lac = -1;
                        if (!entries.isEmpty()) {
                            lac = entries.get(entries.size() - 1).getEntryId();
                        }
                        LedgerMetadata metadata = LedgerMetadataBuilder.create()
                            .withEnsembleSize(1)
                            .withWriteQuorumSize(1)
                            .withAckQuorumSize(1)
                            .withPassword(new byte[0])
                            .withDigestType(DigestType.CRC32C)
                            .withClosedState()
                            .withLastEntryId(lac)
                            .withLength(length)
                            .newEnsembleEntry(0, Lists.newArrayList(new BookieSocketAddress("127.0.0.1", 3181)))
                            .build();

                        promise.complete(new MockReadHandle(ledgerId, metadata, entries));
                    });
                return promise;
            }
        };
    }

    @Override
    public DeleteBuilder newDeleteLedgerOp() {
        return null;
    }

    public void close() {
    };

    private class MockReadHandle implements ReadHandle {
        final long id;
        final LedgerMetadata metadata;
        final List<LedgerEntry> entries;

        MockReadHandle(long id, LedgerMetadata metadata, List<LedgerEntry> entries) {
            this.id = id;
            this.metadata = metadata;
            this.entries = entries;
        }
        @Override
        public long getId() {
            return id;
        }
        @Override
        public CompletableFuture<Void> closeAsync() {
            return CompletableFuture.completedFuture(null);
        }
        @Override
        public LedgerMetadata getLedgerMetadata() {
            return metadata;
        }

        @Override
        public CompletableFuture<LedgerEntries> readAsync(long firstEntry, long lastEntry) {
            CompletableFuture<LedgerEntries> promise = new CompletableFuture<>();
            executor.execute(() -> {
                    if (firstEntry < 0 || firstEntry > getLastAddConfirmed()
                        || lastEntry < 0 || lastEntry > getLastAddConfirmed()) {
                        promise.completeExceptionally(new BKException.BKNoSuchEntryException());
                    } else {
                        List<LedgerEntry> toReturn = entries.stream()
                            .filter(e -> e.getEntryId() >= firstEntry && e.getEntryId() <= lastEntry)
                            .map(e -> e.duplicate())
                            .collect(Collectors.toList());
                        promise.complete(LedgerEntriesImpl.create(toReturn));
                    }
                });
            return promise;
        }
        @Override
        public CompletableFuture<LedgerEntries> readUnconfirmedAsync(long firstEntry, long lastEntry) {
            throw new UnsupportedOperationException();
        }
        @Override
        public CompletableFuture<Long> readLastAddConfirmedAsync() {
            throw new UnsupportedOperationException();
        }
        @Override
        public CompletableFuture<Long> tryReadLastAddConfirmedAsync() {
            throw new UnsupportedOperationException();
        }
        @Override
        public long getLastAddConfirmed() {
            if (entries.isEmpty()) {
                return -1L;
            } else {
                return entries.get(entries.size() - 1).getEntryId();
            }
        }
        @Override
        public long getLength() {
            return metadata.getLength();
        }
        @Override
        public boolean isClosed() {
            return metadata.isClosed();
        }
        @Override
        public CompletableFuture<LastConfirmedAndEntry> readLastAddConfirmedAndEntryAsync(long entryId,
                                                                                          long timeOutInMillis,
                boolean parallel) {
            throw new UnsupportedOperationException();
        }
    }

    private class MockWriteHandle extends MockReadHandle implements WriteHandle {
        MockWriteHandle(long id, LedgerMetadata metadata, List<LedgerEntry> entries) {
            super(id, metadata, entries);
        }

        @Override
        public CompletableFuture<Long> appendAsync(ByteBuf entryData) {
            return CompletableFuture.supplyAsync(() -> {
                    long entryId = entries.size();
                    entries.add(LedgerEntryImpl.create(getId(), entryId, -1, entryData));
                    return entryId;
                }, executor);
        }

        @Override
        public long getLastAddPushed() {
            throw new UnsupportedOperationException();
        }

        @Override
        public CompletableFuture<Void> force() {
            throw new UnsupportedOperationException();
        }
    }
}

