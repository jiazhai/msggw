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

import com.google.common.util.concurrent.AbstractService;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;

import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerFactoryImpl;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.conf.ClientConfiguration;

public class MLTableConsumerGroupStorageService extends AbstractService implements ConsumerGroupStorage {
    private final String zkConnectString;
    private final int ensembleSize;
    private final int writeQuorumSize;
    private final int ackQuorumSize;
    private ManagedLedgerFactory mlFactory;
    private BookKeeper bookkeeper;
    private ExecutorService executor;
    private MLTableConsumerGroupStorage storage;

    public MLTableConsumerGroupStorageService(
            String zkConnectString, int ensembleSize,
            int writeQuorumSize, int ackQuorumSize) {
        this.zkConnectString = zkConnectString;
        this.ensembleSize = ensembleSize;
        this.writeQuorumSize = writeQuorumSize;
        this.ackQuorumSize = ackQuorumSize;
    }

    @Override
    protected void doStart() {
        try {
            ClientConfiguration conf = new ClientConfiguration().setZkServers(zkConnectString);
            ManagedLedgerFactoryImpl mlFactory = new ManagedLedgerFactoryImpl(conf);
            this.mlFactory = mlFactory;
            this.bookkeeper = mlFactory.getBookKeeper();
            this.executor = Executors.newSingleThreadExecutor(new ThreadFactoryBuilder()
                                                              .setNameFormat("ml-cgroup-store-")
                                                              .setUncaughtExceptionHandler((thread, exception) -> {
                                                              })
                                                              .build());
            storage = new MLTableConsumerGroupStorage(mlFactory, bookkeeper,
                    executor, ensembleSize, writeQuorumSize, ackQuorumSize);
            notifyStarted();
        } catch (Exception e) {
            notifyFailed(e);
        }
    }

    @Override
    protected void doStop() {
        try {
            if (this.bookkeeper != null) {
                this.bookkeeper.close();
            }
            if (this.executor != null) {
                executor.shutdownNow();
            }
            notifyStopped();
        } catch (Exception e) {
            notifyFailed(e);
        }
    }

    @Override
    public CompletableFuture<Handle> read(String group) {
        return storage.read(group);
    }
}
