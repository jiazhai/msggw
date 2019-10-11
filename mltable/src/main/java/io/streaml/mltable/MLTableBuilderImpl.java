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
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.client.api.BookKeeper;

import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerFactoryImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerInitializeLedgerCallbackPromise;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MLTableBuilderImpl implements MLTableBuilder {
    private final static Logger log = LoggerFactory.getLogger(MLTableBuilderImpl.class);
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
    public CompletableFuture<MLTable> build() {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(tableName), "table name must be a non-empty string");
        Preconditions.checkArgument(executor != null, "executor must be set");
        Preconditions.checkArgument(mlFactory != null, "managed ledger factory must be set");
        Preconditions.checkArgument(bookkeeper != null, "bookkeeper client must be set");

        ManagedLedgerConfig mlConf = new ManagedLedgerConfig()
            .setEnsembleSize(2).setWriteQuorumSize(2).setAckQuorumSize(2);
        OrderedExecutor orderedExecutor;
        OrderedScheduler scheduledExecutor;

        try {
            Field orderedExecutorField = mlFactory.getClass().getDeclaredField("orderedExecutor");
            orderedExecutorField.setAccessible(true);
            orderedExecutor = (OrderedExecutor)orderedExecutorField.get(mlFactory);

            Field scheduledExecutorField = mlFactory.getClass().getDeclaredField("scheduledExecutor");
            scheduledExecutorField.setAccessible(true);
            scheduledExecutor = (OrderedScheduler)scheduledExecutorField.get(mlFactory);
        } catch (Exception e) {
            CompletableFuture<MLTable> failPromise = new CompletableFuture<>();
            failPromise.completeExceptionally(e);
            return failPromise;
        }

        ManagedLedgerFactoryImpl factoryImpl = (ManagedLedgerFactoryImpl)mlFactory;
        ManagedLedgerImpl ml = new ManagedLedgerImpl(factoryImpl,
                factoryImpl.getBookKeeper(),
                factoryImpl.getMetaStore(),
                mlConf,
                scheduledExecutor,
                orderedExecutor,
                tableName);
        ManagedLedgerInitializeLedgerCallbackPromise initPromise = new ManagedLedgerInitializeLedgerCallbackPromise();
        try {
            Method initMethod = ml.getClass().getDeclaredMethod("initialize",
                    ManagedLedgerInitializeLedgerCallbackPromise.parentClass(),
                    Object.class);
            initMethod.setAccessible(true);
            initMethod.invoke(ml, initPromise, null);
        } catch (Exception e) {
            initPromise.completeExceptionally(e);
        }
        return initPromise.thenCompose(
                (ignore0) -> {
                    MLTableImpl table = new MLTableImpl(tableName, config, ml, bookkeeper, executor);
                    return table.init().thenApply((ignore1) -> table);
                });
    }
}
