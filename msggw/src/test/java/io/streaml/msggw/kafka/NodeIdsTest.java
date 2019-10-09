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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.IntStream;
import java.util.stream.Collectors;

import org.apache.zookeeper.MockZooKeeper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.Ignore;

public class NodeIdsTest {
    private static final Logger log = LoggerFactory.getLogger(NodeIdsTest.class);
    ScheduledExecutorService executor;
    MockZooKeeper zk;

    @Before
    public void setup() throws Exception {
        executor = Executors.newSingleThreadScheduledExecutor(
                new ThreadFactoryBuilder().setNameFormat("test-nodeids").setDaemon(true)
                .setUncaughtExceptionHandler((t, e) -> {
                        log.error("Caught exception in thread {}", t, e);
                }).build());

        zk = MockZooKeeper.newInstance(executor);
    }

    @After
    public void teardown() throws Exception {
        executor.shutdownNow();
    }

    @Test(timeout=100000)
    public void testAllUnique() throws Exception {
        List<CompletableFuture<Integer>> idList = IntStream.range(0, 1000).mapToObj(i -> {
                NodeIds ids = new NodeIdsImpl(executor, zk, "/node-ids");
                return ids.idForNode("node" + i).whenComplete((res, exception) -> {
                        log.info("IKDEBUG node{} got {}", i, res);
                    });
            }).collect(Collectors.toList());

        Set<Integer> idSet = idList.stream().map(CompletableFuture::join)
            .collect(Collectors.toSet());

        assertThat(idSet, hasSize(idList.size()));
    }

    @Test(timeout=10000)
    public void testReread() throws Exception {
        NodeIds ids1 = new NodeIdsImpl(executor, zk, "/node-ids");
        int foobarId = ids1.idForNode("foobar").join();

        NodeIds ids2 = new NodeIdsImpl(executor, zk, "/node-ids");
        int barfooId = ids2.idForNode("barfoo").join();

        assertThat(ids1.idForNode("barfoo").join(), equalTo(barfooId));
    }

    @Test(timeout=10000)
    public void testReregister() throws Exception {
        NodeIds ids1 = new NodeIdsImpl(executor, zk, "/node-ids");
        int foobarId1 = ids1.idForNode("foobar").join();

        NodeIds ids2 = new NodeIdsImpl(executor, zk, "/node-ids");
        int foobarId2 = ids2.idForNode("foobar").join();

        assertThat(foobarId2, equalTo(foobarId1));
    }
}
