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

import com.google.common.collect.ImmutableMap;
import com.google.protobuf.InvalidProtocolBufferException;

import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import io.streaml.msggw.kafka.proto.Kafka.NodeIdList;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NodeIdsImpl implements NodeIds {
    private static final Logger log = LoggerFactory.getLogger(NodeIdsImpl.class);
    private final ScheduledExecutorService scheduler;
    private final ZooKeeper zk;
    private final String znode;
    private final AtomicReference<CachedData> cache = new AtomicReference<>(null);
    private final Random rand;

    public NodeIdsImpl(ScheduledExecutorService scheduler, ZooKeeper zk, String znode) {
        this.scheduler = scheduler;
        this.zk = zk;
        this.znode = znode;
        this.rand = new Random();
    }

    @Override
    public CompletableFuture<Integer> idForNode(String node) {
        CachedData data = cache.get();
        if (data == null) {
            return readOrInitData(node)
                .thenCompose((ignore) -> idForNode(node));
        } else if (!data.ids.containsKey(node)) {
            return addIdForNode(node)
                .thenCompose((ignore) -> idForNode(node));
        } else {
            return CompletableFuture.completedFuture(data.ids.get(node));
        }
    }

    private CompletableFuture<Void> readOrInitData(String node) {
        CompletableFuture<Void> promise = new CompletableFuture<>();
        zk.getData(znode, false,
                   (rc, path, ctx, data, stat) -> {
                       log.debug("[nodeids] readOrInit/getData = {}", rc);
                       if (rc == KeeperException.Code.NONODE.intValue()) {
                           initData(node, promise);
                       } else if (rc == KeeperException.Code.OK.intValue()) {
                           try {
                               cache.set(new CachedData(deserialize(data), stat.getVersion()));
                               promise.complete(null);
                           } catch (Exception e) {
                               promise.completeExceptionally(e);
                           }
                       } else {
                           promise.completeExceptionally(KeeperException.create(rc));
                       }
                   }, null);
        return promise;
    }

    private void initData(String node, CompletableFuture<Void> promise) {
        log.info("[nodeids] Initializing node id data");
        ImmutableMap<String, Integer> idMap = ImmutableMap.of(node, 1);
        zk.create(znode, serialize(idMap), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT,
                  (rc, path, ctx, name) -> {
                      log.debug("[nodeids] initData/create = {}", rc);
                      if (rc == KeeperException.Code.OK.intValue()) {
                          cache.set(new CachedData(idMap, 1));
                          promise.complete(null);
                      } else if (rc == KeeperException.Code.NODEEXISTS.intValue()) {
                          promise.complete(null); // retry read
                      } else {
                          promise.completeExceptionally(KeeperException.create(rc));
                      }
                  }, null);
    }

    private CompletableFuture<Void> addIdForNode(String node) {
        CompletableFuture<Void> promise = new CompletableFuture<>();
        CachedData current = cache.get();
        if (current.ids.containsKey(node)) {
            promise.complete(null);

        } else {
            int id = nextId(current.ids);
            ImmutableMap<String, Integer> newIds = ImmutableMap.<String, Integer>builder()
                .putAll(current.ids)
                .put(node, id).build();
            log.info("[nodeids] Initializing {} to {}", node, id);
            zk.setData(znode, serialize(newIds), current.version,
                       (rc, path, ctx, stat) -> {
                           log.debug("[nodeids] addIdForNode/setData = {}", rc);
                           if (rc == KeeperException.Code.BADVERSION.intValue()) {
                               scheduler.schedule(() -> {
                                       readOrInitData(node).whenCompleteAsync(
                                               (res, exception) -> {
                                                   if (exception != null) {
                                                       promise.completeExceptionally(exception);
                                                   } else {
                                                       promise.complete(res);
                                                   }
                                               });
                                   }, backoffMsWithJitter(), TimeUnit.MILLISECONDS);
                           } else if (rc == KeeperException.Code.OK.intValue()) {
                               cache.set(new CachedData(newIds, stat.getVersion()));
                               promise.complete(null);
                           } else {
                               promise.completeExceptionally(KeeperException.create(rc));
                           }
                       }, null);
        }
        return promise;
    }

    private int backoffMsWithJitter() {
        return rand.nextInt(2000);
    }

    private static int nextId(Map<String, Integer> idMap) {
        return idMap.values().stream().mapToInt(e -> e).max().orElse(0) + 1;
    }

    private static Map<String, Integer> deserialize(byte[] data)
            throws InvalidProtocolBufferException {
        NodeIdList list = NodeIdList.newBuilder().mergeFrom(data).build();
        return list.getIdList().stream().collect(
                ImmutableMap.toImmutableMap(e -> e.getNode(),
                                            e -> e.getNodeId()));
    }

    private static byte[] serialize(Map<String, Integer> idMap) {
        NodeIdList.Builder builder = NodeIdList.newBuilder();
        idMap.entrySet().forEach(e ->
                builder.addIdBuilder().setNode(e.getKey()).setNodeId(e.getValue()));
        return builder.build().toByteArray();
    }

    private static class CachedData {
        private final Map<String, Integer> ids;
        private final int version;

        CachedData(Map<String, Integer> ids,
                   int version) {
            this.ids = ids;
            this.version = version;
        }
    }
}
