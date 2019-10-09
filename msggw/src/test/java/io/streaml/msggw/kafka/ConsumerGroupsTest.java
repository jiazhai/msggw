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

import com.google.common.collect.ImmutableMap;
import com.google.common.testing.FakeTicker;
import com.google.protobuf.ByteString;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.errors.InconsistentGroupProtocolException;
import org.apache.kafka.common.errors.NotCoordinatorException;
import org.apache.kafka.common.errors.UnknownMemberIdException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

public class ConsumerGroupsTest {
    final static Logger log = LoggerFactory.getLogger(ConsumerGroupsTest.class);
    final static ImmutableMap<String, ByteString> protocols = ImmutableMap.of("foo", ByteString.copyFromUtf8("bar"));
    final private static String NAMESPACE = "ns1";
    FakeTicker ticker = new FakeTicker();

    Executor executor = Executors.newSingleThreadExecutor((r) -> {
            Thread t = new Thread(r, "consumers");
            t.setDaemon(true);
            return t;
        });

    MockConsumerGroupStorage storage = new MockConsumerGroupStorage();
    MockCHashGroup chashGroup = new MockCHashGroup("self");

    @Test(timeout=10000)
    public void testRebalanceTimeout() throws Exception {
        ConsumerGroupsImpl groups = new ConsumerGroupsImpl(chashGroup, ticker, storage,
                                                           executor);

        log.info("member1 joins group");
        groups.joinGroup(NAMESPACE, "group", "member1", Integer.MAX_VALUE/2, 100, protocols).join();
        assertThat(groups.getCurrentState(NAMESPACE, "group"), instanceOf(ConsumerGroupsImpl.SyncingState.class));

        log.info("member2 joins group");
        CompletableFuture<ConsumerGroups.State> statef = groups.joinGroup(NAMESPACE, "group", "member2",
                                                                          Integer.MAX_VALUE/2, 100, protocols);
        assertThat(groups.getCurrentState(NAMESPACE, "group"), instanceOf(ConsumerGroupsImpl.JoiningState.class));
        assertThat(statef.isDone(), is(false));

        log.info("pass rebalance timeout before member1 rejoins");
        ticker.advance(101, TimeUnit.MILLISECONDS);
        groups.processTimeouts().join();

        log.info("Only member2 left in group");
        statef.get(10, TimeUnit.SECONDS);
        assertThat(groups.getCurrentState(NAMESPACE, "group"), instanceOf(ConsumerGroupsImpl.SyncingState.class));
        assertThat(groups.getCurrentMembers(NAMESPACE, "group"), containsInAnyOrder("member2"));
    }

    @Test(timeout=10000)
    public void testSessionTimeout() throws Exception {
        ConsumerGroupsImpl groups = new ConsumerGroupsImpl(chashGroup, ticker, storage,
                                                           executor);

        log.info("Join 3 members to the group");
        groups.joinGroup(NAMESPACE, "group", "member1", 100, Integer.MAX_VALUE/2, protocols);
        assertThat(groups.getCurrentState(NAMESPACE, "group"), instanceOf(ConsumerGroupsImpl.SyncingState.class));

        groups.joinGroup(NAMESPACE, "group", "member2", 200, Integer.MAX_VALUE/2, protocols);
        assertThat(groups.getCurrentState(NAMESPACE, "group"), instanceOf(ConsumerGroupsImpl.JoiningState.class));

        CompletableFuture<ConsumerGroups.State> statef =
            groups.joinGroup(NAMESPACE, "group", "member3", 300, Integer.MAX_VALUE/2, protocols);
        assertThat(groups.getCurrentState(NAMESPACE, "group"), instanceOf(ConsumerGroupsImpl.JoiningState.class));

        log.info("Member 1 must rejoin to see all members");
        groups.joinGroup(NAMESPACE, "group", "member1", 100, Integer.MAX_VALUE/2, protocols).join();
        ConsumerGroups.State state = statef.join();
        assertThat(groups.getCurrentState(NAMESPACE, "group"), instanceOf(ConsumerGroupsImpl.SyncingState.class));
        assertThat(groups.getCurrentMembers(NAMESPACE, "group"), containsInAnyOrder("member1", "member2", "member3"));

        log.info("Timeout member1");
        ticker.advance(101, TimeUnit.MILLISECONDS);
        groups.processTimeouts().join();
        assertThat(groups.getCurrentState(NAMESPACE, "group"), instanceOf(ConsumerGroupsImpl.JoiningState.class));
        assertThat(groups.getCurrentMembers(NAMESPACE, "group"), containsInAnyOrder("member2", "member3"));

        log.info("Rejoin member3 and timeout member2 in joining state");
        CompletableFuture<ConsumerGroups.State> state1f = groups.joinGroup(NAMESPACE, "group", "member3", 300,
                                                                           Integer.MAX_VALUE/2, protocols);
        ticker.advance(100, TimeUnit.MILLISECONDS);
        groups.processTimeouts().join();
        assertThat(groups.getCurrentState(NAMESPACE, "group"), instanceOf(ConsumerGroupsImpl.SyncingState.class));
        assertThat(groups.getCurrentMembers(NAMESPACE, "group"), containsInAnyOrder("member3"));
        ConsumerGroups.State state1 = state1f.join();

        log.info("Send assignments from leader {}", state1.getLeader());
        groups.syncGroup(NAMESPACE, "group", state1.getLeader(), state1.getGeneration(),
                         ImmutableMap.of("member31", ByteString.copyFromUtf8("blah1"))).join();
        assertThat(groups.getCurrentMembers(NAMESPACE, "group"), containsInAnyOrder("member3"));
        assertThat(groups.getCurrentState(NAMESPACE, "group"), instanceOf(ConsumerGroupsImpl.StableState.class));

        log.info("Timeout member3");
        ticker.advance(100, TimeUnit.MILLISECONDS);
        groups.processTimeouts().join();
        assertThat(groups.getCurrentState(NAMESPACE, "group"), nullValue());
    }

    @Test(timeout=10000)
    public void testSessionTimeoutDuringJoin() throws Exception {
        ConsumerGroupsImpl groups = new ConsumerGroupsImpl(chashGroup, ticker, storage,
                                                           executor);

        log.info("Join 3 members to the group");
        groups.joinGroup(NAMESPACE, "group", "member1", 100, Integer.MAX_VALUE/2, protocols);
        assertThat(groups.getCurrentState(NAMESPACE, "group"), instanceOf(ConsumerGroupsImpl.SyncingState.class));

        CompletableFuture<ConsumerGroups.State> statef = groups.joinGroup(NAMESPACE, "group", "member2", 200,
                                                                          Integer.MAX_VALUE/2, protocols);
        assertThat(groups.getCurrentState(NAMESPACE, "group"), instanceOf(ConsumerGroupsImpl.JoiningState.class));
        assertThat(statef.isDone(), is(false));

        ticker.advance(101, TimeUnit.MILLISECONDS);
        groups.processTimeouts().join();
        statef.get(10, TimeUnit.SECONDS);
        assertThat(groups.getCurrentState(NAMESPACE, "group"), instanceOf(ConsumerGroupsImpl.SyncingState.class));
        assertThat(groups.getCurrentMembers(NAMESPACE, "group"), containsInAnyOrder("member2"));
    }

    @Test(timeout=10000)
    public void testHeartbeat() throws Exception {
        ConsumerGroupsImpl groups = new ConsumerGroupsImpl(chashGroup, ticker, storage,
                                                           executor);

        log.info("Join member to group with short timeouts");
        ConsumerGroups.State state = groups.joinGroup(NAMESPACE, "group", "member1",
                                                      100 /* session timeout */, 200 /* rebalance timeout */,
                                                      protocols).join();
        assertThat(groups.getCurrentMembers(NAMESPACE, "group"), containsInAnyOrder("member1"));
        assertThat(state, instanceOf(ConsumerGroupsImpl.SyncingState.class));

        log.info("Processing timeouts doesn't timeout member");
        ticker.advance(50, TimeUnit.MILLISECONDS);
        groups.processTimeouts().join();
        assertThat(groups.getCurrentState(NAMESPACE, "group"), instanceOf(ConsumerGroupsImpl.SyncingState.class));
        assertThat(groups.getCurrentMembers(NAMESPACE, "group"), containsInAnyOrder("member1"));

        log.info("Trigger a heartbeat");
        groups.heartbeat(NAMESPACE, "group", "member1", state.getGeneration());
        assertThat(groups.getCurrentState(NAMESPACE, "group"), instanceOf(ConsumerGroupsImpl.SyncingState.class));
        assertThat(groups.getCurrentMembers(NAMESPACE, "group"), containsInAnyOrder("member1"));

        log.info("Going way original timeout doesn't timeout member");
        ticker.advance(100, TimeUnit.MILLISECONDS);
        groups.processTimeouts().join();
        assertThat(groups.getCurrentState(NAMESPACE, "group"), instanceOf(ConsumerGroupsImpl.SyncingState.class));
        assertThat(groups.getCurrentMembers(NAMESPACE, "group"), containsInAnyOrder("member1"));

        log.info("Move to stable state");
        groups.syncGroup(NAMESPACE, "group", state.getLeader(), state.getGeneration(),
                         ImmutableMap.of("member1", ByteString.copyFromUtf8("blah"))).join();
        groups.processTimeouts().join();
        assertThat(groups.getCurrentMembers(NAMESPACE, "group"), containsInAnyOrder("member1"));

        log.info("Trigger a heartbeat in stable state");
        groups.heartbeat(NAMESPACE, "group", "member1", state.getGeneration());
        assertThat(groups.getCurrentMembers(NAMESPACE, "group"), containsInAnyOrder("member1"));

        log.info("Timeout in stable state");
        ticker.advance(150, TimeUnit.MILLISECONDS);
        groups.processTimeouts().join();
        assertThat(groups.getCurrentState(NAMESPACE, "group"), nullValue());
    }

    @Test(timeout=10000)
    public void testJoinAndLeaveGroup() throws Exception {
        ConsumerGroupsImpl groups = new ConsumerGroupsImpl(chashGroup, ticker, storage,
                                                           executor);

        log.info("One member joins group");
        ConsumerGroups.State state1 = groups.joinGroup(NAMESPACE, "group", "member1",
                                                      100 /* session timeout */, 200 /* rebalance timeout */,
                                                      protocols).join();
        assertThat(state1.getMemberData().keySet(), containsInAnyOrder("member1"));

        log.info("Second member joins group");
        CompletableFuture<ConsumerGroups.State> state2f = groups.joinGroup(NAMESPACE, "group", "member2",
                100 /* session timeout */, 200 /* rebalance timeot */,
                protocols);
        assertThat(state2f.isDone(), is(false));
        log.info("First member rejoins");
        ConsumerGroups.State state3 = groups.joinGroup(NAMESPACE, "group", "member1", 100, 200, protocols).join();
        assertThat(state2f.join().getMemberData().keySet(), containsInAnyOrder("member1", "member2"));
        assertThat(state2f.join().getGeneration(), greaterThan(state1.getGeneration()));

        log.info("Group should be in syncing state");
        assertThat(state2f.join(), instanceOf(ConsumerGroupsImpl.SyncingState.class));

        log.info("First member leaves group");
        CompletableFuture<ConsumerGroups.State> state3f = groups.leaveGroup(NAMESPACE, "group", "member1");
        assertThat(state3f.join().getMemberData().keySet(), containsInAnyOrder("member2"));

        log.info("Should still be in joining state, generation shouldn't have changed");
        assertThat(state3f.join().getGeneration(), is(state2f.join().getGeneration()));
        assertThat(state3f.join(), instanceOf(ConsumerGroupsImpl.JoiningState.class));

        log.info("Second member rejoins to see that first has left");
        ConsumerGroups.State state4 = groups.joinGroup(NAMESPACE, "group", "member2", 100, 200, protocols).join();
        assertThat(state4.getGeneration(), greaterThan(state2f.join().getGeneration()));
        assertThat(state4, instanceOf(ConsumerGroupsImpl.SyncingState.class));

        log.info("Third member joins group");
        CompletableFuture<ConsumerGroups.State> state5f = groups.joinGroup(NAMESPACE, "group", "member3",
                                                                           100, 200, protocols);
        assertThat(state5f.isDone(), is(false));

        log.info("Second member rejoins to see new member");
        ConsumerGroups.State state6 = groups.joinGroup(NAMESPACE, "group", "member2", 100, 200, protocols).join();
        state5f.get(5, TimeUnit.SECONDS);

        assertThat(state6.getMemberData().keySet(), containsInAnyOrder("member3", "member2"));
        assertThat(state6.getGeneration(), greaterThan(state4.getGeneration()));
        assertThat(state6, instanceOf(ConsumerGroupsImpl.SyncingState.class));

        log.info("Third member leaves group");
        ConsumerGroups.State state7 = groups.leaveGroup(NAMESPACE, "group", "member3").join();
        assertThat(state7.getMemberData().keySet(), containsInAnyOrder("member2"));
        assertThat(state7, instanceOf(ConsumerGroupsImpl.JoiningState.class));

        log.info("Second member leaves group");
        ConsumerGroups.State state8 = groups.leaveGroup(NAMESPACE, "group", "member2").join();
        assertThat(state8, instanceOf(ConsumerGroupsImpl.EmptyState.class));
    }

    @Test(timeout=10000)
    public void testNoSuitableProtocol() throws Exception {
        ImmutableMap<String, ByteString> protocols1 = ImmutableMap.of("foo", ByteString.copyFromUtf8("blah"));
        ImmutableMap<String, ByteString> protocols2 = ImmutableMap.of("bar", ByteString.copyFromUtf8("blah"));

        ConsumerGroupsImpl groups = new ConsumerGroupsImpl(chashGroup, ticker, storage,
                                                           executor);

        log.info("One member joins group");
        ConsumerGroups.State state1 = groups.joinGroup(NAMESPACE, "group", "member1",
                                                       100 /* session timeout */, 200 /* rebalance timeout */,
                                                       protocols1).join();
        assertThat(state1.getMemberData().keySet(), containsInAnyOrder("member1"));

        log.info("Second member shouldn't be able to join with incompatible protocols");
        CompletableFuture<ConsumerGroups.State> future = groups.joinGroup(NAMESPACE, "group", "member2",
                                                                          100 /* session timeout */,
                                                                          200 /* rebalance timeout */,
                                                                          protocols2);
        try {
            future.join();
            Assert.fail("Shouldn't get here");
        } catch (CompletionException e) {
            assertThat(e.getCause(), instanceOf(InconsistentGroupProtocolException.class));
        }
    }

    @Test(timeout=10000)
    public void testLeavingGroupNotMember() throws Exception {
        ConsumerGroupsImpl groups = new ConsumerGroupsImpl(chashGroup, ticker, storage,
                                                           executor);

        log.info("One member joins group");
        ConsumerGroups.State state1 = groups.joinGroup(NAMESPACE, "group", "member1",
                                                       100 /* session timeout */, 200 /* rebalance timeout */,
                                                       protocols).join();
        assertThat(state1.getMemberData().keySet(), containsInAnyOrder("member1"));

        log.info("Another leaves the group, though it isn't a member");
        CompletableFuture<ConsumerGroups.State> future = groups.leaveGroup(NAMESPACE, "group", "member2");

        try {
            future.join();
            Assert.fail("Shouldn't get here");
        } catch (CompletionException e) {
            assertThat(e.getCause(), instanceOf(UnknownMemberIdException.class));
        }
        assertThat(groups.getCurrentMembers(NAMESPACE, "group"), containsInAnyOrder("member1"));
    }

    @Test(timeout=10000)
    public void testJoiningGroupDoesntCompleteUntilAllKnownJoined() throws Exception {
        ConsumerGroupsImpl groups = new ConsumerGroupsImpl(chashGroup, ticker, storage,
                                                           executor);

        log.info("One member joins group");
        ConsumerGroups.State state1 = groups.joinGroup(NAMESPACE, "group", "member1",
                                                       100 /* session timeout */, 200 /* rebalance timeout */,
                                                       protocols).join();
        assertThat(state1.getMemberData().keySet(), containsInAnyOrder("member1"));

        log.info("Second member joins group");
        CompletableFuture<ConsumerGroups.State> future = groups.joinGroup(NAMESPACE, "group", "member2",
                                                                          100 /* session timeout */,
                                                                          200 /* rebalance timeout */,
                                                                          protocols);

        log.info("Second join shouldn't complete");
        assertThat(future.isDone(), is(false));

        log.info("First member rejoins");
        groups.joinGroup(NAMESPACE, "group", "member1",
                         100 /* session timeout */, 200 /* rebalance timeout */,
                         protocols).get();

        future.get();
    }

    @Test(timeout=10000)
    public void testConsumerGroupStorageRejoinInSyncing() throws Exception {
        ConsumerGroupsImpl groupsInitial = new ConsumerGroupsImpl(chashGroup, ticker, storage,
                                                                  executor);

        log.info("One member joins group");
        ConsumerGroups.State state1 = groupsInitial.joinGroup(NAMESPACE, "group", "member1",
                                                              100 /* session timeout */, 200 /* rebalance timeout */,
                                                              protocols).join();
        assertThat(state1.getMemberData().keySet(), containsInAnyOrder("member1"));
        assertThat(groupsInitial.getCurrentState(NAMESPACE, "group"), instanceOf(ConsumerGroupsImpl.SyncingState.class));

        log.info("Coordinator restarts");
        ConsumerGroupsImpl groupsRestarted = new ConsumerGroupsImpl(chashGroup, ticker, storage,
                                                                    executor);

        log.info("Member syncs with group");
        groupsRestarted.syncGroup(NAMESPACE, "group", state1.getLeader(), state1.getGeneration(),
                                  ImmutableMap.of("member1", ByteString.copyFromUtf8("blah1"))).join();
        assertThat(groupsRestarted.getCurrentMembers(NAMESPACE, "group"), containsInAnyOrder("member1"));
        assertThat(groupsRestarted.getCurrentState(NAMESPACE, "group"), instanceOf(ConsumerGroupsImpl.StableState.class));
        assertThat(groupsRestarted.getCurrentState(NAMESPACE, "group").getGeneration(),
                   equalTo(state1.getGeneration()));
    }

    @Test(timeout=10000)
    public void testConsumerGroupStorageRejoinInStable() throws Exception {
        ConsumerGroupsImpl groupsInitial = new ConsumerGroupsImpl(chashGroup, ticker, storage,
                                                                  executor);

        log.info("One member joins group");
        ConsumerGroups.State state1 = groupsInitial.joinGroup(NAMESPACE, "group", "member1",
                                                              100 /* session timeout */, 200 /* rebalance timeout */,
                                                              protocols).join();
        assertThat(state1.getMemberData().keySet(), containsInAnyOrder("member1"));
        assertThat(groupsInitial.getCurrentState(NAMESPACE, "group"), instanceOf(ConsumerGroupsImpl.SyncingState.class));
        log.info("Member syncs with group");
        ByteString assignment1 = groupsInitial.syncGroup(
                NAMESPACE, "group", state1.getLeader(), state1.getGeneration(),
                ImmutableMap.of("member1", ByteString.copyFromUtf8("blah1"))).join();
        assertThat(groupsInitial.getCurrentMembers(NAMESPACE, "group"), containsInAnyOrder("member1"));
        assertThat(groupsInitial.getCurrentState(NAMESPACE, "group"), instanceOf(ConsumerGroupsImpl.StableState.class));

        int preRestartGeneration = groupsInitial.getCurrentState(NAMESPACE, "group").getGeneration();

        log.info("Coordinator restarts");
        ConsumerGroupsImpl groupsRestarted = new ConsumerGroupsImpl(chashGroup, ticker, storage,
                                                                    executor);

        ByteString assignment2 = groupsRestarted.syncGroup(
                NAMESPACE, "group", state1.getLeader(), state1.getGeneration(),
                ImmutableMap.of("member1", ByteString.copyFromUtf8("blah1"))).join();
        assertThat(assignment2, equalTo(assignment1));
        assertThat(groupsRestarted.getCurrentState(NAMESPACE, "group").getGeneration(),
                   equalTo(preRestartGeneration));
    }

    @Test(timeout=10000)
    public void testConsumerGroupStorageConflictBeforeRejoin() throws Exception {
        ConsumerGroupsImpl groupsInitial = new ConsumerGroupsImpl(chashGroup, ticker, storage,
                executor);

        log.info("One member joins group");
        ConsumerGroups.State state1 = groupsInitial.joinGroup(NAMESPACE, "group", "member1",
                                                              100 /* session timeout */, 200 /* rebalance timeout */,
                                                              protocols).join();
        assertThat(state1.getMemberData().keySet(), containsInAnyOrder("member1"));
        assertThat(groupsInitial.getCurrentState(NAMESPACE, "group"), instanceOf(ConsumerGroupsImpl.SyncingState.class));

        log.info("Make a conflicting write");
        storage.read(ConsumerGroups.groupId(NAMESPACE, "group"))
            .thenCompose((handle) -> handle.write(handle.getAssignment()))
            .join();

        log.info("Member syncs with group");
        try {
            groupsInitial.syncGroup(
                    NAMESPACE, "group", state1.getLeader(), state1.getGeneration(),
                    ImmutableMap.of("member1", ByteString.copyFromUtf8("blah1"))).join();
        } catch (CompletionException e) {
            assertThat(e.getCause(), instanceOf(ConsumerGroupStorage.BadVersionException.class));
        }

        log.info("Run periodical cleanup task");
        groupsInitial.processTimeouts().join();

        assertThat(groupsInitial.getCurrentState(NAMESPACE, "group"),
                   nullValue());

        log.info("Member can try to sync again without issue");
        groupsInitial.syncGroup(
                NAMESPACE, "group", state1.getLeader(), state1.getGeneration(),
                ImmutableMap.of("member1", ByteString.copyFromUtf8("blah1"))).join();
    }

    @Test(timeout=10000)
    public void testConsumerGroupStorageConflictAfterRejoin() throws Exception {
        ConsumerGroupsImpl groupsInitial = new ConsumerGroupsImpl(chashGroup, ticker, storage, executor);

        log.info("One member joins group");
        ConsumerGroups.State state1 = groupsInitial.joinGroup(NAMESPACE, "group", "member1",
                                                              100 /* session timeout */, 200 /* rebalance timeout */,
                                                              protocols).join();
        assertThat(state1.getMemberData().keySet(), containsInAnyOrder("member1"));
        assertThat(groupsInitial.getCurrentState(NAMESPACE, "group"), instanceOf(ConsumerGroupsImpl.SyncingState.class));
        log.info("Member syncs with group");
        groupsInitial.syncGroup(
                NAMESPACE, "group", state1.getLeader(), state1.getGeneration(),
                ImmutableMap.of("member1", ByteString.copyFromUtf8("blah1"))).join();
        assertThat(groupsInitial.getCurrentMembers(NAMESPACE, "group"), containsInAnyOrder("member1"));
        assertThat(groupsInitial.getCurrentState(NAMESPACE, "group"), instanceOf(ConsumerGroupsImpl.StableState.class));

        log.info("Make a conflicting write");
        storage.read(ConsumerGroups.groupId(NAMESPACE, "group"))
            .thenCompose((handle) -> handle.write(handle.getAssignment()))
            .join();

        log.info("Another member joins, triggers joining state");
        groupsInitial.joinGroup(NAMESPACE, "group", "member2",
                                      100 /* session timeout */, 200 /* rebalance timeout */,
                                      protocols);
        assertThat(groupsInitial.getCurrentState(NAMESPACE, "group"),
                   instanceOf(ConsumerGroupsImpl.JoiningState.class));
        try {
            groupsInitial.joinGroup(NAMESPACE, "group", "member1",
                                    100 /* session timeout */, 200 /* rebalance timeout */,
                                    protocols).join();
        } catch (CompletionException e) {
            assertThat(e.getCause(), instanceOf(ConsumerGroupStorage.BadVersionException.class));
        }
        log.info("Run periodical cleanup task");
        groupsInitial.processTimeouts().join();

        assertThat(groupsInitial.getCurrentState(NAMESPACE, "group"),
                   nullValue());

        log.info("Member can try to join again without issue");
        ConsumerGroups.State state2 = groupsInitial.joinGroup(NAMESPACE, "group", "member1",
                                                              100 /* session timeout */, 200 /* rebalance timeout */,
                                                              protocols).join();
        assertThat(state2.getMemberData().keySet(), containsInAnyOrder("member1"));
        assertThat(groupsInitial.getCurrentState(NAMESPACE, "group"), instanceOf(ConsumerGroupsImpl.SyncingState.class));
    }

    @Test(timeout=30000)
    public void testAssignmentChange() throws Exception {
        ConsumerGroupsImpl groups = new ConsumerGroupsImpl(chashGroup, ticker, storage, executor);

        log.info("One member joins group");
        ConsumerGroups.State state1 = groups.joinGroup(NAMESPACE, "group", "member1",
                100 /* session timeout */, 200 /* rebalance timeout */,
                protocols).join();
        assertThat(state1.getMemberData().keySet(), containsInAnyOrder("member1"));
        assertThat(groups.getCurrentState(NAMESPACE, "group"),
                   instanceOf(ConsumerGroupsImpl.SyncingState.class));

        log.info("Run the timeouts, state shouldn't change");
        groups.processTimeouts().join();
        assertThat(groups.getCurrentState(NAMESPACE, "group"),
                   instanceOf(ConsumerGroupsImpl.SyncingState.class));

        log.info("Assign to new node");
        chashGroup.addAssignmentForResource("someothernode:1234", ConsumerGroups.groupId(NAMESPACE, "group"));
        groups.processTimeouts().join();

        log.info("State should be empty");
        assertThat(groups.getCurrentState(NAMESPACE, "group"),
                   instanceOf(ConsumerGroupsImpl.EmptyState.class));

        log.info("Joining should trigger an error");
        try {
            groups.joinGroup(NAMESPACE, "group", "member1",
                             100 /* session timeout */, 200 /* rebalance timeout */,
                             protocols).join();
            Assert.fail("Shouldn't be able to join");
        } catch (CompletionException ce) {
            assertThat(ce.getCause(), instanceOf(NotCoordinatorException.class));
        }

        log.info("Change assignment back");
        chashGroup.addAssignmentForResource(chashGroup.localIdentity(), ConsumerGroups.groupId(NAMESPACE, "group"));

        log.info("Join should work");
        ConsumerGroups.State state2 = groups.joinGroup(NAMESPACE, "group", "member1",
                100 /* session timeout */, 200 /* rebalance timeout */,
                protocols).join();
        assertThat(state2.getMemberData().keySet(), containsInAnyOrder("member1"));
        assertThat(groups.getCurrentState(NAMESPACE, "group"),
                   instanceOf(ConsumerGroupsImpl.SyncingState.class));

    }
}
