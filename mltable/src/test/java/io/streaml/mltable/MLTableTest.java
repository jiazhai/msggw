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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.util.concurrent.CompletionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.junit.Assert;
import org.junit.Test;
import org.junit.Before;
import org.junit.After;

public class MLTableTest {
    private static final Logger log = LoggerFactory.getLogger(MLTableTest.class);
    private MockBKCluster cluster;
    private MockBKClient bkc;
    private ExecutorService executor;

    @Before
    public void setup() throws Exception {
        cluster = new MockBKCluster();
        cluster.start();

        executor = Executors.newSingleThreadExecutor(
                new ThreadFactoryBuilder().setNameFormat("test-").setUncaughtExceptionHandler(
                        (t, e) -> {
                            log.error("Caught exception in thread {}", t, e);
                            }).build());
        bkc = new MockBKClient(executor);
    }

    @After
    public void teardown() throws Exception {
        executor.shutdownNow();
        if (cluster != null) {
            cluster.stop();
        }
    }

    @Test(timeout=10000)
    public void testCRUD() throws Exception {
        long bazVersion = -1;
        try (MLTable mlTable = MLTable.newBuilder()
                .withTableName("testtable").withExecutor(executor)
                .withBookKeeperClient(bkc)
                .withManagedLedgerFactory(cluster.getFactory())
                .build().join()) {
            mlTable.put("foo", "bar".getBytes(UTF_8), MLTable.NEW).join();
            long barVersion = mlTable.put("bar", "foo".getBytes(UTF_8), MLTable.NEW).join();
            bazVersion = mlTable.put("baz", "bar".getBytes(UTF_8), MLTable.NEW).join();

            assertThat(mlTable.get("foo").join().value(), equalTo("bar".getBytes(UTF_8)));
            assertThat(mlTable.get("bar").join().value(), equalTo("foo".getBytes(UTF_8)));
            mlTable.delete("bar", barVersion).join();
            assertThat(mlTable.get("bar").join(), nullValue());
            assertThat(mlTable.get("baz").join().value(), equalTo("bar".getBytes(UTF_8)));

            try {
                mlTable.put("baz", "bar".getBytes(UTF_8), MLTable.NEW).join();
                Assert.fail("Shouldn't succeed");
            } catch (CompletionException e) {
                assertThat(e.getCause(), instanceOf(MLTable.BadVersionException.class));
            }

            try {
                mlTable.put("baz", "bar".getBytes(UTF_8), bazVersion + 1).join();
                Assert.fail("Shoudn't succeed");
            } catch (CompletionException e) {
                assertThat(e.getCause(), instanceOf(MLTable.BadVersionException.class));
            }
        }

        // loads the table back
        try (MLTable mlTable = MLTable.newBuilder()
                .withTableName("testtable").withExecutor(executor)
                .withBookKeeperClient(bkc)
                .withManagedLedgerFactory(cluster.getFactory())
                .build().join()) {
            assertThat(mlTable.get("foo").join().value(), equalTo("bar".getBytes(UTF_8)));
            assertThat(mlTable.get("bar").join(), nullValue());
            assertThat(mlTable.get("baz").join().value(), equalTo("bar".getBytes(UTF_8)));

            try {
                mlTable.put("baz", "bar".getBytes(UTF_8), bazVersion + 1).join();
                Assert.fail("Shoudn't succeed");
            } catch (CompletionException e) {
                assertThat(e.getCause(), instanceOf(MLTable.BadVersionException.class));
            }
            mlTable.put("baz", "foo".getBytes(UTF_8), bazVersion).join();
            assertThat(mlTable.get("baz").join().value(), equalTo("foo".getBytes(UTF_8)));
            assertThat(mlTable.get("baz").join().version(), greaterThan(bazVersion));
        }
    }

    @Test(timeout=10000)
    public void testSnapshot() throws Exception {
        log.info("Create the table and write some entries");
        try (MLTableImpl mlTableImpl = (MLTableImpl)MLTable.newBuilder()
                .withSnapshotAfterNUpdates(1000)
                .withTableName("testtable").withExecutor(executor)
                .withBookKeeperClient(bkc)
                .withManagedLedgerFactory(cluster.getFactory())
                .build().join()) {
            for (int i = 0; i < 500; i++) {
                mlTableImpl.put("foo" + i, "bar".getBytes(UTF_8), MLTable.NEW).join();
            }

            assertThat(cluster.getFactory().getManagedLedgerInfo("testtable").ledgers, is(not(empty())));
            assertThat(mlTableImpl.getSnapshotFuture(), nullValue());

            for (int i = 0; i < 500; i++) {
                mlTableImpl.put("bar" + i, "foo".getBytes(UTF_8), MLTable.NEW).join();
            }
            mlTableImpl.getSnapshotFuture().join();
        }

        log.info("Reloading the table, make sure data is there");
        try (MLTableImpl mlTableImpl = (MLTableImpl)MLTable.newBuilder()
                .withSnapshotAfterNUpdates(1000)
                .withTableName("testtable").withExecutor(executor)
                .withBookKeeperClient(bkc)
                .withManagedLedgerFactory(cluster.getFactory())
                .build().join()) {
            for (int i = 0; i < 500; i++) {
                assertThat(mlTableImpl.get("bar" + i).join().value(), equalTo("foo".getBytes(UTF_8)));
            }
            for (int i = 0; i < 500; i++) {
                assertThat(mlTableImpl.get("foo" + i).join().value(), equalTo("bar".getBytes(UTF_8)));
            }
        }
    }

    // TEST: Test that if a snapshot fails, the next one cleans up in progress
}
