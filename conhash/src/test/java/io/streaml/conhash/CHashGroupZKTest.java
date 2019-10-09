package io.streaml.conhash;

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

import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.stream.IntStream;
import java.util.stream.Collectors;

import org.apache.zookeeper.MockZooKeeper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.Ignore;

public class CHashGroupZKTest {
    private static final Logger log = LoggerFactory.getLogger(CHashGroupZKTest.class);
    private final String rootPath = "/chash";
    private final Consumer<Throwable> fatalHandler = (t) -> { log.error("Fatal error", t); };
    ExecutorService executor;
    MockZooKeeper zk;

    @Before
    public void setup() throws Exception {

        executor = Executors.newSingleThreadExecutor(
                new ThreadFactoryBuilder().setNameFormat("test-chash").setDaemon(true)
                .setUncaughtExceptionHandler((t, e) -> {
                        log.error("Caught exception in thread {}", t, e);
                }).build());

        zk = MockZooKeeper.newInstance(executor);
    }

    @After
    public void teardown() throws Exception {
        executor.shutdownNow();
    }

    @Test(timeout=10000)
    public void testAssignment() throws Exception {
        CHashGroupZKImpl member1 = new CHashGroupZKImpl("member1", rootPath, zk,
                                                        executor, fatalHandler);
        CHashGroupZKImpl member2 = new CHashGroupZKImpl("member2", rootPath, zk,
                                                        executor, fatalHandler);
        CHashGroupZKImpl member3 = new CHashGroupZKImpl("member3", rootPath, zk,
                                                        executor, fatalHandler);

        startMembers(member1, member2, member3);
        awaitMembersSeeMembers(member1, member2, member3);

        for (int i = 0; i < 10000; i++) {
            assertAssignmentMatch("resource" + i, member1, member2, member3);
        }

        stopMembers(member1, member2, member3);
    }

    private void assertAssignmentMatch(String resource, CHashGroup... members) throws Exception {
        String assignment = null;
        for (CHashGroup m : members) {
            if (assignment == null) {
                assignment = m.currentState().lookupOwner(resource);
            }
            assertThat(assignment, equalTo(m.currentState().lookupOwner(resource)));
        }
    }

    @Test(timeout=10000)
    public void testDistribution() throws Exception {
        log.info("Start 3 members in the ring");
        CHashGroupZKImpl member1 = new CHashGroupZKImpl("member1", rootPath, zk,
                                                        executor, fatalHandler);
        CHashGroupZKImpl member2 = new CHashGroupZKImpl("member2", rootPath, zk,
                                                        executor, fatalHandler);
        CHashGroupZKImpl member3 = new CHashGroupZKImpl("member3", rootPath, zk,
                                                        executor, fatalHandler);

        startMembers(member1, member2, member3);
        awaitMembersSeeMembers(member1, member2, member3);

        log.info("Collect the number of resources assigned to each member");
        Map<String, String> assignments = IntStream.range(0, 10000)
            .mapToObj(i -> "resource" + i)
            .collect(Collectors.toMap(
                             resource -> resource,
                             resource -> member1.currentState().lookupOwner(resource)));
        Map<String, Integer> counts = new HashMap<>();
        assignments.entrySet().forEach((e) -> {
                counts.compute(e.getValue(),
                               (key, value) -> {
                                   if (value == null) {
                                       return 1;
                                   } else {
                                       return value + 1;
                                   }
                               });
            });

        log.info("Check that each members gets roughly a third");
        assertThat(((double)assignments.size())/counts.get("member1"),
                   allOf(greaterThan(2.5), lessThan(4.0)));
        assertThat(((double)assignments.size())/counts.get("member2"),
                   allOf(greaterThan(2.5), lessThan(4.0)));
        assertThat(((double)assignments.size())/counts.get("member3"),
                   allOf(greaterThan(2.5), lessThan(4.0)));

        log.info("Add another member to the ring");
        CHashGroupZKImpl member4 = new CHashGroupZKImpl("member4", rootPath, zk,
                                                        executor, fatalHandler);
        startMembers(member4);
        awaitMembersSeeMembers(member1, member2, member3, member4);

        log.info("Assert that only roughly a quarter of all assignments changed, and they changed to the new member");
        Map<String, String> assignments2 = IntStream.range(0, 10000)
            .mapToObj(i -> "resource" + i)
            .collect(Collectors.toMap(
                             resource -> resource,
                             resource -> member1.currentState().lookupOwner(resource)));
        int changed = 0;
        for (String resource : assignments.keySet()) {
            if (!assignments.get(resource).equals(assignments2.get(resource))) {
                changed++;
                assertThat(assignments2.get(resource), equalTo("member4"));
            }
        }
        assertThat(((double)changed)/assignments.size(), allOf(greaterThan(0.2), lessThan(0.3)));

        log.info("Stop one of the first members");
        stopMembers(member2);
        awaitMembersSeeMembers(member1, member3, member4);

        log.info("Assert that only the assignments to member2 changed");
        Map<String, String> assignments3 = IntStream.range(0, 10000)
            .mapToObj(i -> "resource" + i)
            .collect(Collectors.toMap(
                             resource -> resource,
                             resource -> member1.currentState().lookupOwner(resource)));
        for (String resource : assignments2.keySet()) {
            if (!assignments2.get(resource).equals(assignments3.get(resource))) {
                assertThat(assignments2.get(resource), equalTo("member2"));
            }
        }

        log.info("Stop the rest of the members");
        stopMembers(member1, member3, member4);
    }

    private static void startMembers(CHashGroupService... services) throws Exception {
        for (CHashGroupService s : services) {
            s.startAsync();
            s.awaitRunning();
        }
    }

    private static void stopMembers(CHashGroupService... services) throws Exception {
        for (CHashGroupService s : services) {
            s.stopAsync();
            s.awaitTerminated();
        }
    }

    private static void awaitMembersSeeMembers(CHashGroupZKImpl... members) throws Exception {
        for (CHashGroupZKImpl m : members) {
            for (int i = 0; i < 100; i++) {
                if (m.currentState().allOwners().size() == members.length) {
                    break;
                }
                Thread.sleep(100);
            }
            assertThat(m.currentState().allOwners().size(), equalTo(members.length));
        }
    }

    @Test(timeout=10000)
    public void testNotification() throws Exception {
        AtomicBoolean changed = new AtomicBoolean(false);
        CHashGroupZKImpl member1 = new CHashGroupZKImpl("member1", rootPath, zk,
                                                        executor, fatalHandler);
        Runnable listener = () -> { changed.set(true); };
        member1.registerListener(listener);
        assertThat(changed.get(), equalTo(false));
        CHashGroupZKImpl member2 = new CHashGroupZKImpl("member2", rootPath, zk,
                                                        executor, fatalHandler);
        startMembers(member1, member2);
        awaitMembersSeeMembers(member1, member2);

        assertThat(changed.get(), equalTo(true));
        changed.set(false);

        stopMembers(member2);
        awaitMembersSeeMembers(member1);

        assertThat(changed.get(), equalTo(true));
        changed.set(false);

        member1.unregisterListener(listener);

        CHashGroupZKImpl member3 = new CHashGroupZKImpl("member3", rootPath, zk,
                                                        executor, fatalHandler);
        startMembers(member3);
        awaitMembersSeeMembers(member1, member3);

        assertThat(changed.get(), equalTo(false));
    }

}
