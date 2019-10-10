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

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import org.apache.bookkeeper.client.api.BookKeeper;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.bookkeeper.mledger.AsyncCallbacks.OpenLedgerCallback;

public class MLTableBuilderImpl implements MLTableBuilder {
    private String tableName = null;
    private ExecutorService executor = null;
    private ManagedLedgerFactory mlFactory = null;
    private BookKeeper bookkeeper = null;
    private MLTableConfig config = new MLTableConfig();

    @Override
    public MLTableBuilder withTableName(String tableName) {
        this.tableName = tableName;
        return this;
    }

    @Override
    public MLTableBuilder withExecutor(ExecutorService executor) {
        this.executor = executor;
        return this;
    }

    @Override
    public MLTableBuilder withManagedLedgerFactory(ManagedLedgerFactory factory) {
        this.mlFactory = factory;
        return this;
    }

    @Override
    public MLTableBuilder withSnapshotAfterNUpdates(int numUpdates) {
        this.config.numUpdatesForSnapshot = numUpdates;
        return this;
    }

    @Override
    public MLTableBuilder withBookKeeperClient(BookKeeper bkc) {
        this.bookkeeper = bkc;
        return this;
    }

    @Override
    public MLTableBuilder withEnsembleSize(int size) {
        this.config.ensembleSize = size;
        return this;
    }

    @Override
    public MLTableBuilder withWriteQuorumSize(int size) {
        this.config.writeQuorumSize = size;
        return this;
    }

    @Override
    public MLTableBuilder withAckQuorumSize(int size) {
        this.config.ackQuorumSize = size;
        return this;
    }

    @Override
    public CompletableFuture<MLTable> build() {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(tableName), "table name must be a non-empty string");
        Preconditions.checkArgument(executor != null, "executor must be set");
        Preconditions.checkArgument(mlFactory != null, "managed ledger factory must be set");
        Preconditions.checkArgument(bookkeeper != null, "bookkeeper client must be set");

        ManagedLedgerConfig mlConf = new ManagedLedgerConfig()
                .setEnsembleSize(config.ensembleSize)
                .setWriteQuorumSize(config.writeQuorumSize)
                .setAckQuorumSize(config.ackQuorumSize);

        CompletableFuture<ManagedLedger> openPromise = new CompletableFuture<>();
        mlFactory.asyncOpen(tableName, mlConf,
                            new OpenLedgerCallback() {
                                @Override
                                public void openLedgerComplete(ManagedLedger ledger, Object ctx) {
                                    openPromise.complete(ledger);
                                }
                                @Override
                                public void openLedgerFailed(ManagedLedgerException exception, Object ctx) {
                                    openPromise.completeExceptionally(exception);
                                }
                            }, null);
        return openPromise.thenCompose(
                (ledger) -> {
                    MLTableImpl table = new MLTableImpl(tableName, config, ledger, bookkeeper, executor);
                    return table.init().thenApply((ignore) -> table);
                });
    }
}
