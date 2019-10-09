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

import static java.nio.charset.StandardCharsets.UTF_8;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Lists;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.google.common.util.concurrent.AbstractService;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.data.Stat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CHashGroupZKImpl extends AbstractService implements CHashGroupService {
    private final static Logger log = LoggerFactory.getLogger(CHashGroupZKImpl.class);
    private final static int SLOTS = 200;
    private final String advertisedName;
    private final String znode;
    private final String rootPath;
    private final Executor executor;
    private final ZooKeeper zk;
    private final Consumer<Throwable> fatalErrorHandler;
    private final AtomicReference<CHashGroup.State> state;
    private final AtomicBoolean stopping = new AtomicBoolean(false);
    private final List<Runnable> listeners = new CopyOnWriteArrayList<>();

    public CHashGroupZKImpl(String advertisedName, String rootPath,
                            ZooKeeper zk, Executor executor,
                            Consumer<Throwable> fatalErrorHandler) {
        Preconditions.checkArgument(rootPath.startsWith("/"),
                                    "root path must start with /");
        Preconditions.checkArgument(!rootPath.endsWith("/"),
                                    "root path must not end with /");
        this.advertisedName = advertisedName;

        znode = rootPath + "/" + advertisedName;
        this.zk = zk;
        this.rootPath = rootPath;
        this.executor = executor;
        this.fatalErrorHandler = fatalErrorHandler;

        state = new AtomicReference<>(EMPTY_STATE);
    }

    @Override
    public String localIdentity() {
        return advertisedName;
    }

    @Override
    public CHashGroup.State currentState() {
        return state.get();
    }

    @Override
    public void registerListener(Runnable r) {
        listeners.add(r);
    }

    @Override
    public void unregisterListener(Runnable r) {
        listeners.remove(r); // O(n) but there shouldn't be many listeners
    }

    private void buildRing(List<String> members) {
        if (members.isEmpty()) {
            state.set(EMPTY_STATE);
        } else {
            state.set(new StateImpl(members));
        }
        for (Runnable r : listeners) {
            r.run();
        }
    }

    private void populateRing() {
        AsyncCallback.ChildrenCallback cb = new AsyncCallback.ChildrenCallback() {
                @Override
                public void processResult(int rc, String path, Object ctx,
                                          List<String> children) {
                    if (rc == KeeperException.Code.OK.intValue()) {
                        buildRing(children);
                    } else if (rc != KeeperException.Code.CONNECTIONLOSS.intValue()) {
                        fatalErrorHandler.accept(KeeperException.create(rc));
                    } else {
                        populateRing();
                    }
                }
            };
        Watcher w = new Watcher() {
                @Override
                public void process(WatchedEvent event) {
                    log.debug("[chash:{}] Watcher triggered {}", advertisedName, event);
                    if (event.getState() == Watcher.Event.KeeperState.Expired) {
                        fatalErrorHandler.accept(new KeeperException.SessionExpiredException());
                    } else if (!stopping.get()) {
                        zk.getChildren(rootPath, this, cb, null);
                    }
                }
            };
        log.info("Calling get children");
        zk.getChildren(rootPath, w, cb, null);
    }

    @Override
    protected void doStart() {
        ensureRoot()
            .thenComposeAsync((ignore) -> createZNode(), executor)
            .whenCompleteAsync((result, exception) -> {
                    if (exception != null) {
                        notifyFailed(exception);
                        fatalErrorHandler.accept(exception);
                    } else {
                        notifyStarted();

                        populateRing();
                    }
                }, executor);
    }

    @Override
    protected void doStop() {
        stopping.set(true);
        deleteZNode()
            .whenCompleteAsync((result, exception) -> {
                    if (exception != null) {
                        notifyFailed(exception);
                        fatalErrorHandler.accept(exception);
                    } else {
                        notifyStopped();
                    }
                }, executor);
    }

    private CompletableFuture<Void> ensureRoot() {
        CompletableFuture<Void> promise = new CompletableFuture<>();
        zk.exists(rootPath, false,
                  new AsyncCallback.StatCallback() {
                      @Override
                      public void processResult(int rc, String path, Object ctx, Stat stat) {
                          if (rc == KeeperException.Code.OK.intValue() && stat != null) {
                              promise.complete(null);
                          } else if ((rc == KeeperException.Code.OK.intValue() && stat == null)
                                     || (rc == KeeperException.Code.NONODE.intValue())) {
                              createPathOptimistic(rootPath)
                                  .whenCompleteAsync((ignore, exception) -> {
                                          if (exception != null) {
                                              promise.completeExceptionally(exception);
                                          } else {
                                              promise.complete(null);
                                          }
                                      });
                          } else {
                              promise.completeExceptionally(KeeperException.create(rc));
                          }
                      }
                  }, null);
        return promise;
    }

    private CompletableFuture<Void> createPathOptimistic(String path) {
        log.info("[chash:{}] Creating path {}", advertisedName, path);
        CompletableFuture<Void> promise = new CompletableFuture<>();
        zk.create(path, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT,
                  new AsyncCallback.StringCallback() {
                      @Override
                      public void processResult(int rc, String path2, Object ctx, String name) {
                          if (rc == KeeperException.Code.NODEEXISTS.intValue()) {
                              promise.complete(null);
                          } else if (rc == KeeperException.Code.NONODE.intValue()) {
                              String parent = path.substring(0, path.lastIndexOf("/"));
                              if (Strings.isNullOrEmpty(parent)) {
                                  log.error("Couldn't create znode at root, bailing");
                                  promise.completeExceptionally(KeeperException.create(rc));
                              } else {
                                  createPathOptimistic(parent)
                                      .thenComposeAsync(
                                              (ignore) -> createPathOptimistic(path), executor)
                                      .whenCompleteAsync(
                                              (ignore, exception) -> {
                                                  if (exception != null) {
                                                      promise.completeExceptionally(exception);
                                                  } else {
                                                      promise.complete(null);
                                                  }
                                              });
                              }
                          } else if (rc == KeeperException.Code.OK.intValue()) {
                              promise.complete(null);
                          } else {
                              promise.completeExceptionally(KeeperException.create(rc));
                          }
                      }
                  }, null);
        return promise;
    }

    private CompletableFuture<Void> createZNode() {
        log.info("[chash:{}] registering znode", advertisedName);
        CompletableFuture<Void> promise = new CompletableFuture<>();
        zk.create(znode, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL,
                  new AsyncCallback.StringCallback() {
                      @Override
                      public void processResult(int rc, String path, Object ctx, String name) {
                          if (rc == KeeperException.Code.NODEEXISTS.intValue()) {
                              waitForDeletion()
                                  .thenComposeAsync((ignore) -> createZNode(), executor)
                                  .whenCompleteAsync((ignore, exception) -> {
                                          if (exception != null) {
                                              promise.completeExceptionally(exception);
                                          } else {
                                              promise.complete(null);
                                          }
                                      }, executor);
                          } else if (rc == KeeperException.Code.OK.intValue()) {
                              promise.complete(null);
                          } else {
                              promise.completeExceptionally(KeeperException.create(rc));
                          }
                      }
                  }, null);
        return promise;
    }

    private CompletableFuture<Void> waitForDeletion() {
        log.info("[chash:{}] znode exists, waiting for it to delete", advertisedName);
        CompletableFuture<Void> promise = new CompletableFuture<>();
        AsyncCallback.StatCallback cb = new AsyncCallback.StatCallback() {
                @Override
                public void processResult(int rc, String path, Object ctx, Stat stat) {
                    if (rc != KeeperException.Code.OK.intValue()) {
                        promise.completeExceptionally(KeeperException.create(rc));
                    } else if (stat == null) {
                        promise.complete(null);
                    }
                }
            };
        Watcher w = new Watcher() {
                @Override
                public void process(WatchedEvent event) {
                    if (event.getState() == Watcher.Event.KeeperState.Expired) {
                        promise.completeExceptionally(new KeeperException.SessionExpiredException());
                    } else if (event.getType() == Watcher.Event.EventType.NodeDeleted) {
                        log.info("[chash:{}] znode deleted", advertisedName);
                        promise.complete(null);
                    } else {
                        zk.exists(znode, this, cb, null);
                    }
                }
            };
        zk.exists(znode, w, cb, null);
        return promise;
    }

    private CompletableFuture<Void> deleteZNode() {
        log.info("[chash:{}] deleting znode", advertisedName);
        CompletableFuture<Void> promise = new CompletableFuture<>();
        zk.delete(znode, -1,
                new AsyncCallback.VoidCallback() {
                    @Override
                    public void processResult(int rc, String path, Object ctx) {
                        if (rc != KeeperException.Code.OK.intValue()) {
                            promise.completeExceptionally(KeeperException.create(rc));
                        } else {
                            promise.complete(null);
                        }
                    }
                  }, null);
        return promise;
    }

    private final CHashGroup.State EMPTY_STATE = new CHashGroup.State() {
            @Override
            public String lookupOwner(String resource) {
                return localIdentity();
            }
            @Override
            public List<String> allOwners() {
                return Lists.newArrayList(localIdentity());
            }
        };

    private class StateImpl implements CHashGroup.State {
        final List<String> members;
        final NavigableMap<Long, String> ring;

        StateImpl(List<String> members) {
            ImmutableSortedMap.Builder builder = ImmutableSortedMap.<Long,String>naturalOrder();
            for (String member : members) {
                HashFunction hash = Hashing.murmur3_128();
                long seed = hash.hashString(member, UTF_8).asLong();
                for (int i = 0; i < SLOTS; i++) {
                    long next = hash.hashLong(seed).asLong();
                    builder.put(next, member);
                    seed = next;
                }
            }
            this.members = members.stream().sorted().collect(ImmutableList.toImmutableList());
            this.ring = builder.build();

            if (log.isTraceEnabled()) {
                log.trace("[chash:{}] >>>> NEW RING >>>>", localIdentity());
                for (Map.Entry<Long, String> e : this.ring.entrySet()) {
                    log.trace("[chash:{}] {} -> {}", advertisedName, e.getKey(), e.getValue());
                }
                log.trace("[chash:{}] <<<< END NEW RING <<<<", localIdentity());
            }
        }

        @Override
        public String lookupOwner(String resource) {
            HashFunction hash = Hashing.murmur3_128();
            Map.Entry<Long, String> owner = ring.ceilingEntry(hash.hashString(resource, UTF_8).asLong());
            if (owner == null) {
                return ring.firstEntry().getValue();
            } else {
                return owner.getValue();
            }
        }

        @Override
        public List<String> allOwners() {
            return members;
        }
    }
}
