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

import com.google.common.base.Strings;
import com.google.common.base.Ticker;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.ByteString;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.errors.IllegalGenerationException;
import org.apache.kafka.common.errors.InconsistentGroupProtocolException;
import org.apache.kafka.common.errors.NotCoordinatorException;
import org.apache.kafka.common.errors.RebalanceInProgressException;
import org.apache.kafka.common.errors.UnknownMemberIdException;
import org.apache.kafka.common.errors.UnknownServerException;

import io.streaml.conhash.CHashGroup;
import io.streaml.msggw.kafka.proto.Kafka.ConsumerGroupAssignment;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerGroupsImpl implements ConsumerGroups {
    private final static Logger log = LoggerFactory.getLogger(ConsumerGroupsImpl.class);

    static abstract class BaseState implements State {
        private final String groupName;
        private final int generation;
        private final ConsumerGroupStorage.Handle storageHandle;

        BaseState(String groupName, int generation,
                  ConsumerGroupStorage.Handle storageHandle) {
            this.groupName = groupName;
            this.generation = generation;
            this.storageHandle = storageHandle;
        }

        @Override
        public String getGroupName() {
            return groupName;
        }

        @Override
        public int getGeneration() {
            return generation;
        }

        @Override
        public String getCurrentProtocol() {
            throw new IllegalStateException("Can only select when joined");
        }

        @Override
        public String getLeader() {
            throw new IllegalStateException("Can only select when joined");
        }

        @Override
        public ImmutableMap<String, ByteString> getMemberProtocolMetadata() {
            throw new IllegalStateException("Can only select when joined");
        }

        @Override
        public CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> getOffsetAndMetadata(
                Collection<TopicPartition> topics) {
            return storageHandle.getOffsetAndMetadata(topics);
        }

        @Override
        public CompletableFuture<Void> putOffsetAndMetadata(int generation,
                                                            Map<TopicPartition, OffsetAndMetadata> offsets) {
            if (getGeneration() != generation) {
                CompletableFuture<Void> promise = new CompletableFuture<>();
                promise.completeExceptionally(new IllegalGenerationException());
                return promise;
            }
            return storageHandle.putOffsetAndMetadata(offsets);
        }

        @Override
        public ConsumerGroupStorage.Handle getStorageHandle() {
            return storageHandle;
        }

        @Override
        public CompletableFuture<State> close() {
            return storageHandle.close().thenApply(ignore -> new EmptyState());
        }
    }

    static class EmptyState implements State {
        @Override
        public String getGroupName() {
            throw new IllegalStateException("Empty state");
        }

        @Override
        public int getGeneration() {
            throw new IllegalStateException("Empty state");
        }

        @Override
        public String getCurrentProtocol() {
            throw new IllegalStateException("Empty state");
        }

        @Override
        public String getLeader() {
            throw new IllegalStateException("Empty state");
        }

        @Override
        public ImmutableMap<String, ByteString> getMemberProtocolMetadata() {
            throw new IllegalStateException("Empty state");
        }

        @Override
        public ImmutableMap<String, MemberData> getMemberData() {
            throw new IllegalStateException("Empty state");
        }

        @Override
        public ConsumerGroupStorage.Handle getStorageHandle() {
            throw new IllegalStateException("Empty state");
        }

        @Override
        public CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> getOffsetAndMetadata(
                Collection<TopicPartition> topics) {
            CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> promise = new CompletableFuture<>();
            promise.completeExceptionally(new IllegalStateException("Empty state"));
            return promise;
        }

        @Override
        public CompletableFuture<Void> putOffsetAndMetadata(int generation,
                                                            Map<TopicPartition, OffsetAndMetadata> offsets) {
            CompletableFuture<Void> promise = new CompletableFuture<>();
            promise.completeExceptionally(new IllegalStateException("Empty state"));
            return promise;
        }

        @Override
        public CompletableFuture<State> close() {
            return CompletableFuture.completedFuture(this);
        }
    }

    interface JoinEventHandler {
        void validateJoin(ImmutableMap<String, ByteString> protocols);
        CompletableFuture<State> join(String name,
                                      int sessionTimeout, int rebalanceTimeout,
                                      ImmutableMap<String, ByteString> protocols,
                                      CompletableFuture<State> promise);
    }

    interface LeaveEventHandler {
        CompletableFuture<State> leave(String name);
    }

    interface SyncEventHandler {
        CompletableFuture<State> sync(String name, int generation,
                                      ImmutableMap<String, ByteString> assignments,
                                      CompletableFuture<ByteString> promise);
    }

    interface HeartbeatEventHandler {
        void heartbeat(String name, int generation);
    }

    interface TimeoutEventHandler {
        CompletableFuture<State> processTimeouts();
    }

    class JoiningState extends BaseState
        implements JoinEventHandler, LeaveEventHandler, HeartbeatEventHandler, SyncEventHandler, TimeoutEventHandler {

        CompletableFuture<State> joinPromise;
        Set<String> joinsOutstanding;
        Map<String, MemberData> newMemberData;
        Map<String, Long> deadlines;
        long rebalanceDeadline;

        JoiningState(String groupName, int generation, ConsumerGroupStorage.Handle storageHandle) {
            this(groupName, generation, storageHandle, ImmutableMap.of(), new ConcurrentHashMap<>());
        }

        JoiningState(String groupName, int generation, ConsumerGroupStorage.Handle storageHandle,
                     ImmutableMap<String, MemberData> existingMemberData,
                     Map<String, Long> deadlines) {
            super(groupName, generation, storageHandle);

            joinsOutstanding = new HashSet<>(existingMemberData.keySet());
            this.joinPromise = new CompletableFuture<>();
            newMemberData = new HashMap<>(existingMemberData);
            this.deadlines = deadlines;

            rebalanceDeadline = currentTickMs() + existingMemberData.values()
                .stream().mapToInt(MemberData::getRebalanceTimeout)
                .max().orElse(5000);
        }

        @Override
        public void validateJoin(ImmutableMap<String, ByteString> protocols) {
            selectProtocol(Stream.of(protocols.keySet()),
                           newMemberData.values().stream().map(
                                   member -> member.getProtocolMetadata().keySet()));
        }

        @Override
        public CompletableFuture<State> join(String name,
                          int sessionTimeout, int rebalanceTimeout,
                          ImmutableMap<String, ByteString> protocols,
                          CompletableFuture<State> promise) {
            joinPromise.whenComplete((state, exception) -> {
                    if (exception != null) {
                        promise.completeExceptionally(exception);
                    } else {
                        promise.complete(state);
                    }
                });

            if (newMemberData.containsKey(name)) {
                joinsOutstanding.remove(name);
                return maybeCompleteJoin();
            }
            MemberData newMember = new MemberData(sessionTimeout, rebalanceTimeout, protocols);
            newMemberData.put(name, newMember);
            deadlines.put(name, currentTickMs() + newMember.getSessionTimeout());
            return maybeCompleteJoin();
        }

        @Override
        public CompletableFuture<State> leave(String name) {
            if (dropMember(name)) {
                return maybeCompleteJoin();
            } else {
                throw new UnknownMemberIdException();
            }
        }

        private boolean dropMember(String name) {
            deadlines.remove(name);
            joinsOutstanding.remove(name);
            return newMemberData.remove(name) != null;
        }

        @Override
        public CompletableFuture<State> sync(String name, int generation,
                                             ImmutableMap<String, ByteString> assignments,
                                             CompletableFuture<ByteString> promise) {
            throw new RebalanceInProgressException();
        }

        @Override
        public void heartbeat(String name, int generation) {
            if (getGeneration() != generation) {
                throw new IllegalGenerationException();
            }
            MemberData data = newMemberData.get(name);
            if (data == null) {
                throw new UnknownMemberIdException();
            } else {
                deadlines.put(name, currentTickMs() + data.getSessionTimeout());
                throw new RebalanceInProgressException();
            }
        }

        @Override
        public CompletableFuture<State> processTimeouts() {
            Set<String> toRemove = deadlines.entrySet().stream()
                .filter(e -> e.getValue() < currentTickMs())
                .map(Map.Entry::getKey)
                .collect(Collectors.toSet());

            if (currentTickMs() > rebalanceDeadline) {
                toRemove.addAll(joinsOutstanding);
            }
            toRemove.forEach(this::dropMember);
            return maybeCompleteJoin();
        }

        CompletableFuture<State> maybeCompleteJoin() {
            if (newMemberData.isEmpty()) {
                // noone should be waiting on this, but give them an exception anyhow
                joinPromise.completeExceptionally(new UnknownMemberIdException());
                return CompletableFuture.completedFuture(new EmptyState());
            } else if (joinsOutstanding.isEmpty()) {
                int newGeneration = getGeneration() + 1;
                ImmutableMap<String, MemberData> memberData = ImmutableMap.copyOf(newMemberData);

                ConsumerGroupAssignment protobuf = consumerGroupProtobuf(
                        newGeneration, memberData, ImmutableMap.of());
                return getStorageHandle().write(protobuf)
                    .thenApplyAsync((result) -> {
                            SyncingState newState = new SyncingState(getGroupName(),
                                                                     newGeneration,
                                                                     getStorageHandle(),
                                                                     memberData,
                                                                     deadlines);
                            joinPromise.complete(newState);
                            return newState;
                        }, executor);
            } else {
                return CompletableFuture.completedFuture(this);
            }
        }

        @Override
        public ImmutableMap<String, MemberData> getMemberData() {
            return ImmutableMap.copyOf(newMemberData);
        }

        @Override
        public CompletableFuture<Void> putOffsetAndMetadata(int generation,
                                                            Map<TopicPartition, OffsetAndMetadata> offsets) {
            return super.putOffsetAndMetadata(generation, offsets)
                .thenCompose((ignore) -> {
                        CompletableFuture<Void> promise = new CompletableFuture<>();
                        promise.completeExceptionally(new RebalanceInProgressException());
                        return promise;
                    });
        }
    }

    class JoinedBaseState extends BaseState
        implements JoinEventHandler, TimeoutEventHandler,
                   LeaveEventHandler, HeartbeatEventHandler {
        private final String leader;
        private final String currentProtocol;
        private final ImmutableMap<String, ByteString> memberProtocolMetadata;
        private final ImmutableMap<String, MemberData> allMemberData;
        private final Map<String, Long> deadlines;

        JoinedBaseState(String groupName,
                        int generation, ConsumerGroupStorage.Handle storageHandle,
                        ImmutableMap<String, MemberData> members,
                        Map<String, Long> deadlines) {
            super(groupName, generation, storageHandle);

            this.allMemberData = members;
            this.currentProtocol = selectProtocol(
                    members.values().stream().map(member -> member.getProtocolMetadata().keySet()));
            this.leader = selectLeader(members.keySet());
            this.memberProtocolMetadata = memberProtocolData(members, currentProtocol);
            this.deadlines = deadlines;
        }

        @Override
        public String getCurrentProtocol() {
            return currentProtocol;
        }

        @Override
        public String getLeader() {
            return leader;
        }

        @Override
        public ImmutableMap<String, ByteString> getMemberProtocolMetadata() {
            return memberProtocolMetadata;
        }

        @Override
        public ImmutableMap<String, MemberData> getMemberData() {
            return allMemberData;
        }

        Map<String, Long> getDeadlines() {
            return deadlines;
        }

        @Override
        public void validateJoin(ImmutableMap<String, ByteString> protocols) {
            selectProtocol(Stream.of(protocols.keySet()),
                           allMemberData.values().stream().map(
                                   member -> member.getProtocolMetadata().keySet()));
        }

        @Override
        public CompletableFuture<State> join(String name,
                          int sessionTimeout, int rebalanceTimeout,
                          ImmutableMap<String, ByteString> protocols,
                          CompletableFuture<State> promise) {
            JoiningState newState = new JoiningState(getGroupName(), getGeneration(),
                                                     getStorageHandle(),
                                                     getMemberData(), getDeadlines());
            return newState.join(name, sessionTimeout, rebalanceTimeout, protocols, promise);
        }

        @Override
        public CompletableFuture<State> leave(String name) {
            JoiningState newState = new JoiningState(getGroupName(), getGeneration(),
                                                     getStorageHandle(),
                                                     getMemberData(), getDeadlines());
            return newState.leave(name);
        }

        @Override
        public void heartbeat(String name, int generation) {
            if (getGeneration() != generation) {
                throw new IllegalGenerationException();
            }
            MemberData data = getMemberData().get(name);
            if (data == null) {
                throw new UnknownMemberIdException();
            } else {
                getDeadlines().put(name, currentTickMs() + data.getSessionTimeout());
            }
        }

        @Override
        public CompletableFuture<State> processTimeouts() {
            long needTimeout = deadlines.entrySet().stream()
                .filter(e -> {
                        return e.getValue() < currentTickMs();
                    }).count();
            if (needTimeout > 0) {
                JoiningState newState = new JoiningState(getGroupName(), getGeneration(),
                                                         getStorageHandle(),
                                                         getMemberData(), getDeadlines());
                return newState.processTimeouts();
            } else {
                return CompletableFuture.completedFuture(this);
            }
        }
    }

    class SyncingState extends JoinedBaseState
        implements SyncEventHandler, LeaveEventHandler, HeartbeatEventHandler {
        CompletableFuture<ImmutableMap<String, ByteString>> assignmentPromise = new CompletableFuture<>();

        SyncingState(String groupName, int generation,
                     ConsumerGroupStorage.Handle storageHandle,
                     ImmutableMap<String, MemberData> members, Map<String, Long> deadlines) {
            super(groupName, generation, storageHandle, members, deadlines);
        }

        @Override
        public CompletableFuture<State> join(String name,
                          int sessionTimeout, int rebalanceTimeout,
                          ImmutableMap<String, ByteString> protocols,
                          CompletableFuture<State> promise) {
            assignmentPromise.completeExceptionally(new RebalanceInProgressException());
            return super.join(name, sessionTimeout, rebalanceTimeout, protocols, promise);
        }

        @Override
        public CompletableFuture<State> leave(String name) {
            assignmentPromise.completeExceptionally(new RebalanceInProgressException());
            return super.leave(name);
        }

        @Override
        public CompletableFuture<State> sync(String name, int generation,
                                             ImmutableMap<String, ByteString> assignments,
                                             CompletableFuture<ByteString> promise) {
            if (getGeneration() != generation) {
                throw new IllegalGenerationException();
            }

            assignmentPromise.whenComplete((result, exception) -> {
                    if (exception != null) {
                        promise.completeExceptionally(exception);
                    } else {
                        promise.complete(result.get(name));
                    }
                });

            if (getLeader().equals(name)) {
                if (log.isDebugEnabled()) {
                    log.debug("Assignments received from leader {} - {}", name, assignments);
                }

                ConsumerGroupAssignment protobuf = consumerGroupProtobuf(
                        getGeneration(), getMemberData(), assignments);
                return getStorageHandle().write(protobuf)
                    .thenApplyAsync((result) -> {
                            assignmentPromise.complete(assignments);
                            return new StableState(getGroupName(),
                                                   getGeneration(),
                                                   getStorageHandle(),
                                                   getMemberData(),
                                                   getDeadlines(),
                                                   assignmentPromise);
                        }, executor);
            } else {
                return CompletableFuture.completedFuture(this);
            }
        }

        @Override
        public void heartbeat(String name, int generation) {
            super.heartbeat(name, generation);
            throw new RebalanceInProgressException();
        }
    }

    class StableState extends JoinedBaseState implements SyncEventHandler {
        private CompletableFuture<ImmutableMap<String, ByteString>> assignmentPromise;

        StableState(String groupName,
                    int generation, ConsumerGroupStorage.Handle storageHandle,
                    ImmutableMap<String, MemberData> memberData,
                    Map<String, Long> deadlines,
                    CompletableFuture<ImmutableMap<String, ByteString>> assigmentPromise) {
            super(groupName, generation, storageHandle, memberData, deadlines);
            this.assignmentPromise = assigmentPromise;
        }

        @Override
        public CompletableFuture<State> sync(String name, int generation,
                                             ImmutableMap<String, ByteString> assignments,
                                             CompletableFuture<ByteString> promise) {
            if (getGeneration() != generation) {
                throw new IllegalGenerationException();
            }

            assignmentPromise.whenComplete((result, exception) -> {
                    if (exception != null) {
                        promise.completeExceptionally(exception);
                    } else {
                        promise.complete(result.get(name));
                    }
                });
            return CompletableFuture.completedFuture(this);
        }
    }

    private final ConcurrentHashMap<String, CompletableFuture<State>> groups = new ConcurrentHashMap<>();
    private final CHashGroup chashGroup;
    private final Ticker ticker;
    private final ConsumerGroupStorage storage;
    private final Executor executor;
    private final AtomicBoolean assignmentsChanged = new AtomicBoolean(false);
    private final AtomicReference<CompletableFuture<ConsumerGroupStorage.Handle>> nullGroupHandle
        = new AtomicReference<>();

    public ConsumerGroupsImpl(CHashGroup chashGroup,
                              Ticker ticker, ConsumerGroupStorage storage,
                              Executor executor) {
        this.chashGroup = chashGroup;
        this.chashGroup.registerListener(() -> assignmentsChanged.set(true));
        this.ticker = ticker;
        this.storage = storage;
        this.executor = executor;
    }

    private void withGroup(String namespace, String group, String eventName,
                           Function<State, CompletableFuture<State>> action,
                           CompletableFuture<?> clientPromise) {
        String fqgn = ConsumerGroups.groupId(namespace, group);
        groups.compute(fqgn,
                       (key, current) -> {
                           CompletableFuture<State> originalState = current;
                           if (current == null) {
                               current = CompletableFuture.completedFuture(new EmptyState());
                           }
                           return current
                               .exceptionally((exception) -> new EmptyState())
                               .thenComposeAsync((state) -> {
                                       if (state instanceof EmptyState) {
                                           if (!chashGroup.currentState()
                                                   .lookupOwner(fqgn)
                                                   .equals(chashGroup.localIdentity())) {
                                               throw new NotCoordinatorException(
                                                       String.format("%s is not coordinator for group %s",
                                                                     chashGroup.localIdentity(),
                                                                     group));
                                           }
                                           return storage.read(fqgn)
                                               .thenApply((consumerGroupData) ->
                                                          buildState(fqgn, consumerGroupData));
                                       } else {
                                           return originalState;
                                       }
                                   }, executor)
                               .thenComposeAsync((state) -> {
                                       try {
                                           return action.apply(state);
                                       } catch (Throwable t) {
                                           clientPromise.completeExceptionally(t);
                                           return originalState;
                                       }
                                   }, executor)
                               .whenCompleteAsync(logTransition(fqgn, eventName, originalState),
                                                  executor);
                       })
            .whenCompleteAsync(propagateErrors(clientPromise), executor);
    }

    static BiConsumer<Object, Throwable> propagateErrors(CompletableFuture<?> promise) {
        return (result, exception) -> {
            if (exception != null) {
                promise.completeExceptionally(exception);
            }
        };
    }

    static <T> BiConsumer<T, Throwable> propagate(CompletableFuture<T> promise) {
        return (result, exception) -> {
            if (exception != null) {
                promise.completeExceptionally(exception);
            } else {
                promise.complete(result);
            }
        };
    }

    @Override
    public CompletableFuture<State> joinGroup(String namespace, String group, String name,
                                              int sessionTimeout, int rebalanceTimeout,
                                              ImmutableMap<String, ByteString> protocols) {
        CompletableFuture<State> promise = new CompletableFuture<>();
        withGroup(namespace, group, "join",
                  (state) -> {
                      if (state instanceof JoinEventHandler) {
                          JoinEventHandler handler = (JoinEventHandler)state;
                          handler.validateJoin(protocols);
                          return handler.join(name, sessionTimeout,
                                              rebalanceTimeout,
                                              protocols, promise);
                      } else {
                          // current state cannot handle join
                          throw new UnknownServerException();
                      }
                  },
                  promise);
        return promise;
    }

    @Override
    public CompletableFuture<State> leaveGroup(String namespace, String group, String name) {
        CompletableFuture<State> promise = new CompletableFuture<>();
        withGroup(namespace, group, "leave",
                  (state) -> {
                      if (state instanceof LeaveEventHandler) {
                          return ((LeaveEventHandler)state).leave(name)
                              .whenComplete(propagate(promise));
                      } else {
                          throw new UnknownServerException();
                      }
                  }, promise);
        return promise;
    }


    @Override
    public CompletableFuture<ByteString> syncGroup(String namespace,
                                                   String group, String name, int generation,
                                                   ImmutableMap<String, ByteString> assignments) {
        CompletableFuture<ByteString> promise = new CompletableFuture<>();
        withGroup(namespace, group, "sync",
                  (state) -> {
                      if (state instanceof SyncEventHandler) {
                          return ((SyncEventHandler)state).sync(name, generation, assignments, promise);
                      } else {
                          // current state cannot handle sync event
                          throw new UnknownServerException();
                      }
                  }, promise);
        return promise;
    }

    @Override
    public CompletableFuture<Void> heartbeat(String namespace, String group,
                                             String name, int generation) {
        CompletableFuture<Void> promise = new CompletableFuture<>();
        withGroup(namespace, group, "heartbeat",
                  (state) -> {
                      if (state instanceof HeartbeatEventHandler) {
                          ((HeartbeatEventHandler)state).heartbeat(name, generation);
                          promise.complete(null);
                          return CompletableFuture.completedFuture(state);
                      } else {
                          // current state cannot handle sync event
                          throw new UnknownServerException();
                      }
                }, promise);
        return promise;
    }

    @Override
    public CompletableFuture<Void> processTimeouts() {
        List<CompletableFuture<?>> futures = new ArrayList<>();
        for (String group : groups.keySet()) {
            CompletableFuture<State> stateF =
                groups.compute(group,
                               (key, current) -> {
                                   if (current == null) {
                                       return serverExceptionPromise();
                                   }
                                   return current.thenComposeAsync(
                                           (state) -> {
                                               if (state instanceof TimeoutEventHandler) {
                                                   return ((TimeoutEventHandler)state).processTimeouts();
                                               } else {
                                                   return current;
                                               }
                                           }, executor)
                                       .whenCompleteAsync(logTransition(group, "timeout", current), executor);
                               })
                .handleAsync((result, exception) -> {
                        if (exception != null || result instanceof EmptyState) {
                            // get from groups to make sure we're still talking of same object
                            CompletableFuture<State> state2 = groups.get(group);
                            if (state2 != null && state2.isDone()) {
                                try {
                                    if (state2.join() instanceof EmptyState) {
                                        groups.remove(group, state2);
                                    }
                                } catch (Throwable t) {
                                    groups.remove(group, state2);
                                }
                            }
                        }
                        return null;
                    }, executor);
            futures.add(stateF);
        }
        if (assignmentsChanged.get()) {
            assignmentsChanged.set(false);
            CHashGroup.State chashState = chashGroup.currentState();
            for (String group : groups.keySet()) {
                CompletableFuture<State> stateF =
                    groups.compute(group, (key, value) -> {
                            if (value != null &&
                                !chashState.lookupOwner(key).equals(chashGroup.localIdentity())) {
                                return value
                                    .thenCompose(state -> state.close());
                            } else {
                                return value;
                            }
                        });
                futures.add(stateF);
            }
        }
        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
    }

    @Override
    public CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> getCommittedOffset(
            String namespace, String group, List<TopicPartition> topics) {
        if (Strings.isNullOrEmpty(group)) {
            return loadOrGetNullGroup()
                .thenComposeAsync(data -> data.getOffsetAndMetadata(topics),
                                  executor);
        } else {
            CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> promise = new CompletableFuture<>();
            groups.compute(ConsumerGroups.groupId(namespace, group), (key, current) -> {
                    if (current == null) {
                        promise.completeExceptionally(
                                new NotCoordinatorException("Not coordinator for group " + group));
                    } else {
                        current.whenCompleteAsync((state, exception) -> {
                                if (exception != null) {
                                    promise.completeExceptionally(exception);
                                } else {
                                    state.getOffsetAndMetadata(topics)
                                        .whenComplete((result, ex) -> {
                                                if (ex != null) {
                                                    promise.completeExceptionally(ex);
                                                } else {
                                                    promise.complete(result);
                                                }
                                            });
                                }
                            }, executor);
                    }
                    return current;
                });
            return promise;
        }
    }

    @Override
    public CompletableFuture<Void> commitOffset(String namespace,
                                                String group, String name,
                                                int generation,
                                                Map<TopicPartition, OffsetAndMetadata> offsets) {
        CompletableFuture<Void> promise = new CompletableFuture<>();
        if (Strings.isNullOrEmpty(group)) {
            return loadOrGetNullGroup().thenComposeAsync(data -> data.putOffsetAndMetadata(offsets),
                                                         executor);
        } else {
            groups.compute(ConsumerGroups.groupId(namespace, group),  (key, current) -> {
                    if (current == null) {
                        promise.completeExceptionally(
                                new NotCoordinatorException("Not coordinator for group " + group));
                    } else {
                        current.whenComplete((state, exception) -> {
                                if (exception != null) {
                                    promise.completeExceptionally(exception);
                                } else {
                                    state.putOffsetAndMetadata(generation, offsets)
                                        .whenComplete((result, exception2) -> {
                                                if (exception2 != null) {
                                                    promise.completeExceptionally(exception2);
                                                } else {
                                                    promise.complete(result);
                                                }
                                            });
                                }
                            });
                    }
                    return current;
                });
        }
        return promise;
    }

    CompletableFuture<State> serverExceptionPromise() {
        CompletableFuture<State> promise = new CompletableFuture<>();
        promise.completeExceptionally(new UnknownServerException());
        return promise;
    }

    private static String nullOrClass(Object o) {
        if (o == null) {
            return "null";
        } else if (o instanceof CompletableFuture) {
            CompletableFuture f = (CompletableFuture)o;
            if (f.isDone()) {
                try {
                    return f.join().getClass().getSimpleName();
                } catch (Throwable t) {
                    return nullOrClass(t);
                }
            } else {
                return f.toString();
            }
        } else {
            return o.getClass().getSimpleName();
        }
    }

    private static <R, T extends Throwable> BiConsumer<R, T> logTransition(String group, String event,
                                                                           CompletableFuture<State> current) {
        if (!log.isDebugEnabled()) {
            return (result, exception) -> {};
        }
        return (result, exception) -> {
            if (exception != null) {
                log.debug("[{}](state:{}) -> {} -> ERROR({})",
                          group, nullOrClass(current.join()), event, nullOrClass(exception), exception);
            } else {
                if (result == null) {
                    log.debug("[{}](state:{}) -> {} -> null", group, nullOrClass(current), event);
                } else if (current != null && current.isDone() && result.equals(current.join())) {
                    log.debug("[{}](state:{}) No transition for event {}", group, nullOrClass(current), event);
                } else {
                    log.debug("[{}](state:{}) -> {} -> (state:{})",
                              group, nullOrClass(current), event, nullOrClass(result));
                }
            }

        };
    }

    private static String selectLeader(Collection<String> allMembers) {
        return allMembers.stream().sorted().findFirst().get();
    }

    private static ImmutableMap<String, ByteString> memberProtocolData(
            Map<String, MemberData> allMemberData, String currentProtocol) {
        ImmutableMap.Builder<String, ByteString> builder = ImmutableMap.builder();
        allMemberData.entrySet().forEach(
                (e) -> builder.put(e.getKey(), e.getValue().getProtocolMetadata().get(currentProtocol)));
        return builder.build();
    }

    private static String selectProtocol(Stream<Set<String>>... protocolMapStreams)
            throws InconsistentGroupProtocolException {
        Stream<Set<String>> acc = Stream.empty();
        for (Stream<Set<String>> s : protocolMapStreams) {
            acc = Stream.concat(acc, s);
        }
        return acc.reduce((left, right) -> {
                Set<String> intersection = new HashSet<>(left);
                intersection.retainAll(right);
                return intersection;
            })
            .orElse(ImmutableSet.of())
            .stream().findFirst()
            .orElseThrow(() -> new InconsistentGroupProtocolException("No protocol exists for all members"));
    }

    State getCurrentState(String namespace, String group) {
        CompletableFuture<State> state = groups.get(ConsumerGroups.groupId(namespace, group));
        if (state == null) {
            return null;
        } else {
            return state.join();
        }
    }

    Set<String> getCurrentMembers(String namespace, String group) {
        return ((BaseState)groups.get(ConsumerGroups.groupId(namespace, group)).join()).getMemberData().keySet();
    }

    long currentTickMs() {
        return TimeUnit.NANOSECONDS.toMillis(ticker.read());
    }

    static ConsumerGroupAssignment consumerGroupProtobuf(
            int generation,
            ImmutableMap<String, MemberData> memberData,
            ImmutableMap<String, ByteString> assignments) {
        ConsumerGroupAssignment.Builder builder = ConsumerGroupAssignment.newBuilder();
        builder.setGeneration(generation);

        memberData.entrySet().forEach(
                e -> {
                    ConsumerGroupAssignment.MemberData.Builder member =
                        builder.addMemberDataBuilder();
                    member.setMember(e.getKey())
                        .setSessionTimeout(e.getValue().getSessionTimeout())
                        .setRebalanceTimeout(e.getValue().getRebalanceTimeout());
                    e.getValue().getProtocolMetadata().entrySet().stream()
                        .forEach((p) -> {
                                member.addProtocolMetadataBuilder()
                                    .setProtocol(p.getKey())
                                    .setMetadata(p.getValue());
                            });
                });
        assignments.entrySet().forEach(
                e -> {
                    builder.addAssignmentBuilder()
                        .setMember(e.getKey())
                        .setAssignment(e.getValue());
                });
        return builder.build();
    }

    private ImmutableMap<String, ByteString> protobufToProtocolMetadata(
            List<ConsumerGroupAssignment.ProtocolMetadata> protobuf) {
        return protobuf.stream()
            .collect(ImmutableMap.toImmutableMap(
                             e -> e.getProtocol(),
                             e -> e.getMetadata()));
    }

    State buildState(String groupName, ConsumerGroupStorage.Handle storageHandle) {
        ConsumerGroupAssignment assignment = storageHandle.getAssignment();
        if (assignment == null) {
            return new JoiningState(groupName, 1, storageHandle);
        }

        ImmutableMap<String, MemberData> memberData = assignment.getMemberDataList().stream()
            .collect(ImmutableMap.toImmutableMap(
                             e -> e.getMember(),
                             e -> new MemberData(e.getSessionTimeout(), e.getRebalanceTimeout(),
                                     protobufToProtocolMetadata(e.getProtocolMetadataList()))));
        ImmutableMap<String, ByteString> assignments = assignment.getAssignmentList().stream()
            .collect(ImmutableMap.toImmutableMap(
                             e -> e.getMember(),
                             e -> e.getAssignment()));

        long currentTickMs = currentTickMs();
        Map<String, Long> deadlines = memberData.entrySet()
            .stream().collect(Collectors.toMap(
                                      e -> e.getKey(),
                                      e -> currentTickMs + e.getValue().getSessionTimeout()/2));
        if (assignments.isEmpty()) {
            return new SyncingState(groupName,
                                    assignment.getGeneration(),
                                    storageHandle,
                                    memberData,
                                    deadlines);
        } else {
            return new StableState(groupName,
                                   assignment.getGeneration(),
                                   storageHandle,
                                   memberData,
                                   deadlines,
                                   CompletableFuture.completedFuture(assignments));
        }
    }

    CompletableFuture<ConsumerGroupStorage.Handle> loadOrGetNullGroup() {
        CompletableFuture<ConsumerGroupStorage.Handle> current = nullGroupHandle.get();
        while (current == null || (current.isDone() && current.isCompletedExceptionally())) {
            CompletableFuture<ConsumerGroupStorage.Handle> promise = new CompletableFuture<>();
            if (nullGroupHandle.compareAndSet(current, promise)) {
                current = promise;
                storage.read("")
                    .whenCompleteAsync((result, exception) -> {
                            if (exception != null) {
                                promise.completeExceptionally(exception);
                            } else {
                                promise.complete(result);
                            }
                        });
            } else {
                current = nullGroupHandle.get();
            }
        }
        return current; // otherwise is not null and either not done or completed successfully
    }
}
