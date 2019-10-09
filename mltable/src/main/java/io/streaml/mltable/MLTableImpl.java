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

import static java.nio.charset.StandardCharsets.UTF_8;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.api.BookKeeper;
import org.apache.bookkeeper.client.api.DigestType;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.client.api.WriteHandle;
import org.apache.bookkeeper.client.api.LedgerEntry;
import org.apache.bookkeeper.mledger.AsyncCallbacks.AddEntryCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.CloseCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.OpenCursorCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.MarkDeleteCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.ReadEntriesCallback;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MLTableImpl implements MLTable {
    private static final Logger log = LoggerFactory.getLogger(MLTableImpl.class);
    private static final String snapshotCursorName = "__snapshotCursor";
    private static final String INPROGRESS_PREFIX = "inprogress-";
    private static final String OLD_SNAPSHOT_PREFIX = "oldsnapshot-";
    private static final String SNAPSHOT_PROPERTY_KEY = "SNAPSHOT";
    private static final byte[] SNAPSHOT_PASSWD = new byte[0];

    private static final int READ_BATCH_SIZE = 2000;
    private static final int WRITESNAPSHOT_BATCH_SIZE = 2000;
    private static final int READSNAPSHOT_BATCH_SIZE = 200;
    private final String name;
    private final MLTableConfig config;
    private final ManagedLedger ledger;
    private final BookKeeper bookkeeper;
    private final ExecutorService executor;
    private final ConcurrentHashMap<String, ValueImpl> map = new ConcurrentHashMap<>();
    private final AtomicReference<CompletableFuture<Void>> snapshotPromise = new AtomicReference<>(null);
    private ManagedCursor snapshotCursor;

    MLTableImpl(String name, MLTableConfig config,
                ManagedLedger ledger,
                BookKeeper bookkeeper,
                ExecutorService executor) {
        this.name = name;
        this.config = config;
        this.ledger = ledger;
        this.bookkeeper = bookkeeper;
        this.executor = executor;
    }

    CompletableFuture<Void> init() {
        CompletableFuture<ManagedCursor> cursorPromise = new CompletableFuture<>();
        ledger.asyncOpenCursor(snapshotCursorName,
                new OpenCursorCallback() {
                    @Override
                    public void openCursorComplete(ManagedCursor cursor, Object ctx) {
                        cursorPromise.complete(cursor);
                    }
                    @Override
                    public void openCursorFailed(ManagedLedgerException exception, Object ctx) {
                        cursorPromise.completeExceptionally(exception);
                    }
                }, null);
        return cursorPromise.thenComposeAsync((cursor) -> {
                snapshotCursor = cursor;
                return loadFromCursor(cursor, map);
            }, executor);
    }

    @Override
    public CompletableFuture<Long> put(String key, byte[] value, long expectedVersion) {
        Formats.KeyValueOp op = Formats.KeyValueOp.newBuilder()
            .setOpType(Formats.KeyValueOp.OpType.PUT)
            .setKey(key).setValue(ByteString.copyFrom(value))
            .setExpectedVersion(expectedVersion).build();
        CompletableFuture<Long> returnPromise = new CompletableFuture<>();
        logOp(op).whenCompleteAsync((position, exception) -> {
                if (exception != null) {
                    returnPromise.completeExceptionally(exception);
                } else {
                    log.debug("[mlt:{}] Update for {} logged at {}", name, key, position);
                    try {
                        returnPromise.complete(
                                putInternal(op.getKey(),
                                            op.getValue(),
                                            op.getExpectedVersion()));
                        maybeSnapshot(position);
                    } catch (BadVersionException bve) {
                        returnPromise.completeExceptionally(bve);
                    }
                }
            }, executor);
        return returnPromise;
    }

    @Override
    public CompletableFuture<Value> get(String key) {
        return CompletableFuture.supplyAsync(() -> map.get(key), executor);
    }

    @Override
    public CompletableFuture<Void> delete(String key, long expectedVersion) {
        Formats.KeyValueOp op = Formats.KeyValueOp.newBuilder()
            .setOpType(Formats.KeyValueOp.OpType.DELETE)
            .setKey(key).setExpectedVersion(expectedVersion).build();
        CompletableFuture<Void> returnPromise = new CompletableFuture<>();
        logOp(op).whenCompleteAsync((position, exception) -> {
                if (exception != null) {
                    returnPromise.completeExceptionally(exception);
                } else {
                    try {
                        deleteInternal(op.getKey(),
                                       op.getExpectedVersion());
                        returnPromise.complete(null);
                        maybeSnapshot(position);
                    } catch (BadVersionException bve) {
                        returnPromise.completeExceptionally(bve);
                    }
                }
            }, executor);
        return returnPromise;
    }

    @Override
    public void close() throws CompletionException {
        closeAsync().join();
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        CompletableFuture<Void> promise = new CompletableFuture<>();
        ledger.asyncClose(
                new CloseCallback() {
                    @Override
                    public void closeComplete(Object ctx) {
                        promise.complete(null);
                    }
                    @Override
                    public void closeFailed(ManagedLedgerException exception, Object ctx) {
                        promise.completeExceptionally(exception);
                    }
                }, null);
        return promise;
    }

    CompletableFuture<Position> logOp(Formats.KeyValueOp op) {
        CompletableFuture<Position> addPromise = new CompletableFuture<>();
        ledger.asyncAddEntry(op.toByteArray(),
                new AddEntryCallback() {
                    @Override
                    public void addComplete(Position position, Object ctx) {
                        addPromise.complete(position);
                    }
                    @Override
                    public void addFailed(ManagedLedgerException exception, Object ctx) {
                        addPromise.completeExceptionally(exception);
                    }
                }, null);
        return addPromise;
    }

    long putInternal(String key, ByteString value, long expectedVersion) throws BadVersionException {
        ValueImpl current = map.get(key);
        if (expectedVersion == NEW) {
            long newVersion = 1;
            if (map.putIfAbsent(key, new ValueImpl(value, newVersion)) == null) {
                return newVersion;
            } else {
                throw new BadVersionException();
            }
        } else if (current != null && current.version() == expectedVersion) {
            long newVersion = expectedVersion + 1;
            if (map.replace(key, current, new ValueImpl(value, newVersion))) {
                return newVersion;
            } else {
                throw new BadVersionException();
            }
        } else {
            throw new BadVersionException();
        }
    }

    void deleteInternal(String key, long expectedVersion) throws BadVersionException {
        Value current = map.get(key);
        if (expectedVersion == ANY) {
            map.remove(key);
        } else if (current != null && current.version() == expectedVersion) {
            if (!map.remove(key, current)) {
                throw new BadVersionException();
            }
        } else {
            throw new BadVersionException();
        }
    }

    private CompletableFuture<Void> loadFromCursor(ManagedCursor cursor,
                                                   ConcurrentHashMap<String, ValueImpl> map) {
        Long snapshotLedger = cursor.getProperties().get(SNAPSHOT_PROPERTY_KEY);
        CompletableFuture<Void> snapshotLoadFuture;
        if (snapshotLedger == null) {
            log.info("[mlt:{}] No snapshot available {}", name, cursor);
            snapshotLoadFuture = CompletableFuture.completedFuture(null);
        } else {
            snapshotLoadFuture = loadSnapshot(snapshotLedger, map);
        }
        return snapshotLoadFuture.thenComposeAsync((ignore) -> loadLogLoop(cursor, map), executor);
    }

    private CompletableFuture<Void> loadSnapshot(long ledgerId, ConcurrentHashMap<String, ValueImpl> map) {
        log.info("[mlt:{}] Loading snapshot from ledger {}", name, ledgerId);
        return bookkeeper.newOpenLedgerOp()
            .withLedgerId(ledgerId)
            .withPassword(SNAPSHOT_PASSWD)
            .withDigestType(DigestType.CRC32C)
            .withRecovery(true)
            .execute()
            .thenCompose((readHandle) -> {
                    CompletableFuture<Void> readPromise = new CompletableFuture<Void>();
                    readSnapshotEntriesLoop(readHandle, 0L, map, readPromise);
                    return readPromise;
                });
    }

    private void readSnapshotEntriesLoop(ReadHandle readHandle, long firstEntry,
                                         ConcurrentHashMap<String, ValueImpl> map,
                                         CompletableFuture<Void> promise) {
        if (firstEntry > readHandle.getLastAddConfirmed()) {
            promise.complete(null);
            return;
        }
        long lastEntry = Math.min(firstEntry + READSNAPSHOT_BATCH_SIZE, readHandle.getLastAddConfirmed());
        readHandle.readAsync(firstEntry, lastEntry)
            .thenAccept((entries) -> {
                    try {
                        for (LedgerEntry e : entries) {
                            Formats.KeyValueSnapshotBatch batch =
                                Formats.KeyValueSnapshotBatch.newBuilder()
                                .mergeFrom(e.getEntryBytes()).build();
                            for (Formats.KeyValueSnapshotBatch.Entry batchEntry : batch.getEntryList()) {
                                map.put(batchEntry.getKey(),
                                        new ValueImpl(batchEntry.getValue(), batchEntry.getVersion()));
                            }
                        }
                    } catch (InvalidProtocolBufferException ipbe) {
                        throw new CompletionException(ipbe);
                    } finally {
                        entries.close();
                    }
                })
            .whenComplete((ignore, exception) -> {
                    if (exception != null) {
                        promise.completeExceptionally(exception);
                    } else {
                        readSnapshotEntriesLoop(readHandle, lastEntry + 1,
                                                map, promise);
                    }
                });
    }

    private CompletableFuture<Void> loadLogLoop(ManagedCursor cursor,
                                                ConcurrentHashMap<String, ValueImpl> map) {
        CompletableFuture<List<Entry>> loadPromise = new CompletableFuture<>();
        cursor.asyncReadEntries(READ_BATCH_SIZE,
                                new ReadEntriesCallback() {
                                    @Override
                                    public void readEntriesComplete(List<Entry> entries, Object ctx) {
                                        loadPromise.complete(entries);
                                    }
                                    public void readEntriesFailed(ManagedLedgerException exception,
                                                                  Object ctx) {
                                        loadPromise.completeExceptionally(exception);
                                    }
                                }, null);
        return loadPromise.thenComposeAsync(
                (entries) -> {
                    try {
                        for (Entry e : entries) {
                            Formats.KeyValueOp kv = Formats.KeyValueOp.newBuilder()
                                .mergeFrom(e.getData()).build();
                            try {
                                switch (kv.getOpType()) {
                                case PUT:
                                    putInternal(kv.getKey(), kv.getValue(), kv.getExpectedVersion());
                                    break;
                                case DELETE:
                                    deleteInternal(kv.getKey(), kv.getExpectedVersion());
                                    break;
                                }
                            } catch (BadVersionException bve) {
                                // not a problem, we only check after we write
                            }
                        }
                    } catch (InvalidProtocolBufferException ipbe) {
                        CompletableFuture error = new CompletableFuture<>();
                        error.completeExceptionally(ipbe);
                        return error;
                    }
                    if (entries.isEmpty()) {
                        return CompletableFuture.completedFuture(null);
                    } else {
                        return loadLogLoop(cursor, map);
                    }
                }, executor);
    }

    private static class ValueImpl implements Value {
        private final ByteString data;
        private final long version;

        ValueImpl(ByteString data, long version) {
            this.data = data;
            this.version = version;
        }

        ByteString data() {
            return data;
        }

        @Override
        public byte[] value() {
            return data.toByteArray();
        }

        @Override
        public long version() {
            return version;
        }
    }

    private boolean shouldSnapshot() {
        return snapshotCursor.getNumberOfEntriesInBacklog() >= config.numUpdatesForSnapshot;
    }

    private void maybeSnapshot(Position position) {
        CompletableFuture<Void> previousSnapshot = snapshotPromise.get();
        if ((previousSnapshot == null || previousSnapshot.isDone()) && shouldSnapshot()) {
            CompletableFuture<Void> promise = new CompletableFuture<Void>();
            if (snapshotPromise.compareAndSet(previousSnapshot, promise)) {
                takeSnapshot(position, promise);
            }
        }
    }

    private static class SnapshotEntry {
        private final String key;
        private final ValueImpl value;

        SnapshotEntry(String key, ValueImpl value) {
            this.key = key;
            this.value = value;
        }
    }

    private void takeSnapshot(Position position, CompletableFuture<Void> promise) {
        List<SnapshotEntry> clone = map.entrySet().stream()
            .map(e -> new SnapshotEntry(e.getKey(), e.getValue())).collect(Collectors.toList());
        Position currentMarkDelete = snapshotCursor.getMarkDeletedPosition();
        log.info("[mlt:{}] Snapshot triggered at position {}", name, position);
        deleteOldSnapshots()
            .thenComposeAsync(
                    (deletedSnapshotKeys) -> {
                        if (!deletedSnapshotKeys.isEmpty()) {
                            log.debug("[mlt:{}] Deleted old snapshots: {}", name, deletedSnapshotKeys);
                        }
                        Map<String,byte[]> metadata = ImmutableMap.of("origin", "mltable".getBytes(UTF_8),
                                                                      "tableName", name.getBytes(UTF_8),
                                                                      "position", position.toString().getBytes(UTF_8));
                        String inprogressTag = INPROGRESS_PREFIX + UUID.randomUUID().toString();

                        Map<String, Long> newProperties = new HashMap<>(snapshotCursor.getProperties());
                        deletedSnapshotKeys.forEach(newProperties::remove);
                        return bookkeeper.newCreateLedgerOp()
                            .withEnsembleSize(2)
                            .withWriteQuorumSize(2)
                            .withAckQuorumSize(2)
                            .withPassword(SNAPSHOT_PASSWD)
                            .withDigestType(DigestType.CRC32C)
                            .withCustomMetadata(metadata)
                            .execute()
                            .thenComposeAsync((snapshotLedger) -> {
                                    log.debug("[mlt:{}] Snapshot will be written to ledger {}", snapshotLedger.getId());
                                    newProperties.put(inprogressTag, snapshotLedger.getId());
                                    return updateCursor(snapshotCursor, currentMarkDelete, newProperties)
                                        .thenComposeAsync((ignore) -> writeMapToLedger(snapshotLedger, clone), executor)
                                        .thenComposeAsync((ignore) -> {
                                                newProperties.remove(inprogressTag);
                                                Long oldSnapshot = newProperties.put(SNAPSHOT_PROPERTY_KEY,
                                                                                     snapshotLedger.getId());
                                                if (oldSnapshot != null) {
                                                    newProperties.put(OLD_SNAPSHOT_PREFIX + UUID.randomUUID(),
                                                                      oldSnapshot);
                                                }
                                                return updateCursor(snapshotCursor, position, newProperties);
                                            }, executor);
                                });
                    })
            .whenCompleteAsync((ignore, exception) -> {
                    if (exception != null) {
                        promise.completeExceptionally(exception);
                    } else {
                        promise.complete(null);
                    }
                }, executor);
    }

    private CompletableFuture<Void> writeMapToLedger(WriteHandle wh, List<SnapshotEntry> snapshotEntries) {
        CompletableFuture<Void> promise = new CompletableFuture<>();
        writeMapToLedgerLoop(wh, snapshotEntries, promise);
        return promise;
    }

    private void writeMapToLedgerLoop(WriteHandle wh, List<SnapshotEntry> snapshotEntries,
                                      CompletableFuture<Void> promise) {
        if (snapshotEntries.isEmpty()) {
            promise.complete(null);
            return;
        }
        Formats.KeyValueSnapshotBatch.Builder batch = Formats.KeyValueSnapshotBatch.newBuilder();
        for (int i = 0; i < WRITESNAPSHOT_BATCH_SIZE && !snapshotEntries.isEmpty(); i++) {
            SnapshotEntry e = snapshotEntries.remove(0);
            batch.addEntryBuilder().setKey(e.key).setValue(e.value.data()).setVersion(e.value.version());
        }
        wh.appendAsync(batch.build().toByteArray())
            .whenComplete((result, exception) -> {
                    if (exception != null) {
                        promise.completeExceptionally(exception);
                    } else {
                        writeMapToLedgerLoop(wh, snapshotEntries, promise);
                    }
                });
    }

    private CompletableFuture<Void> updateCursor(ManagedCursor cursor,
                                                 Position position, Map<String, Long> properties) {
        log.debug("[mlt:{}] Updating cursor {} to position {}", name, cursor.getName(), position);
        CompletableFuture<Void> promise = new CompletableFuture<>();
        cursor.asyncMarkDelete(position, properties, new MarkDeleteCallback() {
                @Override
                public void markDeleteComplete(Object ctx) {
                    promise.complete(null);
                }
                @Override
                public void markDeleteFailed(ManagedLedgerException exception, Object ctx) {
                    promise.completeExceptionally(exception);
                }
            }, null);
        return promise;
    }

    private CompletableFuture<List<String>> deleteOldSnapshots() {
        Map<String, Long> currentProps = snapshotCursor.getProperties();

        Map<String, CompletableFuture<?>> deletions = currentProps.entrySet().stream()
            .filter(e -> e.getKey().startsWith(INPROGRESS_PREFIX) || e.getKey().startsWith(OLD_SNAPSHOT_PREFIX))
            .collect(Collectors.toMap(
                             e -> e.getKey(),
                             e -> bookkeeper.newDeleteLedgerOp().withLedgerId(e.getValue()).execute()));
        CompletableFuture<?> deletionsFuture =
            CompletableFuture.allOf(deletions.values().toArray(new CompletableFuture[0]));
        return deletionsFuture.handleAsync((ignore, exception) -> {
                return deletions.entrySet().stream()
                    .filter(e -> {
                            try {
                                e.getValue().join();
                                return true;
                            } catch (CompletionException ce) {
                                if (ce.getCause() instanceof BKException.BKNoSuchLedgerExistsException) {
                                    return true;
                                } else {
                                    log.warn("[mlt:{}] Unexpected error cleaning up snapshot ledger for {}",
                                             name, e.getKey(), ce);
                                    return false;
                                }
                            }
                        })
                    .map(e -> e.getKey())
                    .collect(Collectors.toList());
            }, executor);
    }

    CompletableFuture<Void> getSnapshotFuture() {
        return snapshotPromise.get();
    }
}
