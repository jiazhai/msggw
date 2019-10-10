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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.kafka.common.protocol.CommonFields.THROTTLE_TIME_MS;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Streams;
import com.google.protobuf.ByteString;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import javax.naming.AuthenticationException;

import io.netty.util.AttributeKey;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.ReferenceCountUtil;

import io.streaml.conhash.CHashGroup;

import io.streaml.msggw.MessagingGatewayConfiguration;
import org.apache.commons.lang3.tuple.Pair;

import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.Records;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.ApiVersionsResponse;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.requests.FetchResponse;
import org.apache.kafka.common.requests.FindCoordinatorRequest;
import org.apache.kafka.common.requests.FindCoordinatorResponse;
import org.apache.kafka.common.requests.HeartbeatRequest;
import org.apache.kafka.common.requests.HeartbeatResponse;
import org.apache.kafka.common.requests.JoinGroupRequest;
import org.apache.kafka.common.requests.JoinGroupResponse;
import org.apache.kafka.common.requests.ListOffsetRequest;
import org.apache.kafka.common.requests.ListOffsetResponse;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.OffsetCommitRequest;
import org.apache.kafka.common.requests.OffsetCommitResponse;
import org.apache.kafka.common.requests.OffsetFetchRequest;
import org.apache.kafka.common.requests.OffsetFetchResponse;
import org.apache.kafka.common.requests.ProduceRequest;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.common.requests.SaslAuthenticateRequest;
import org.apache.kafka.common.requests.SaslAuthenticateResponse;
import org.apache.kafka.common.requests.SaslHandshakeRequest;
import org.apache.kafka.common.requests.SaslHandshakeResponse;
import org.apache.kafka.common.requests.SyncGroupRequest;
import org.apache.kafka.common.requests.SyncGroupResponse;
import org.apache.kafka.common.utils.ByteBufferOutputStream;

import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import org.apache.pulsar.broker.authentication.AuthenticationService;
import org.apache.pulsar.broker.authorization.AuthorizationService;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClient;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;

import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.PersistentTopicInternalStats;

import org.apache.pulsar.common.policies.data.TopicStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Sharable
public class KafkaRequestHandler extends ChannelInboundHandlerAdapter {
    private static final Logger log = LoggerFactory.getLogger(KafkaRequestHandler.class);

    private static final int DEFAULT_FETCH_BUFFER_SIZE = 16*1024;
    private static final int MAX_FETCH_BUFFER_SIZE = 20*1024*1024;

    private final String clusterName;
    private final NodeIds nodeIds;
    private final CHashGroup chashGroup;
    private final PulsarAdmin admin;
    private final PulsarClient client;
    private final ExecutorService executor;
    private final KafkaProducerThread producerThread;
    private final Fetcher fetcher;

    private final ConsumerGroups consumerGroups;
    private final ConcurrentHashMap<Channel, Queue<Completion>> completions = new ConcurrentHashMap<>();
    private final AuthenticationService authenticationService;
    private final Optional<AuthorizationService> authzService;
    private final MessagingGatewayConfiguration config;

    public KafkaRequestHandler(String clusterName,
                               NodeIds nodeIds,
                               CHashGroup chashGroup,
                               PulsarAdmin admin, PulsarClient client,
                               ExecutorService executor, KafkaProducerThread producerThread,
                               Fetcher fetcher, ConsumerGroups consumerGroups,
                               AuthenticationService authenticationService,
                               Optional<AuthorizationService> authzService,
                               MessagingGatewayConfiguration configuration) {
        this.clusterName = clusterName;
        this.nodeIds = nodeIds;
        this.chashGroup = chashGroup;
        this.admin = admin;
        this.client = client;
        this.executor = executor;
        this.producerThread = producerThread;
        this.fetcher = fetcher;
        this.consumerGroups = consumerGroups;
        this.authenticationService = authenticationService;
        this.authzService = authzService;
        this.config = configuration;
    }

    private static Node addressToNode(int id, String address) throws NumberFormatException {
        String[] parts = address.split(":");
        if (parts.length == 2) {
            return new Node(id, parts[0],
                            Integer.valueOf(parts[1]));
        } else {
            throw new IllegalArgumentException("Invalid node registered for kafka {}, skipping");
        }
    }

    private static MetadataResponse.TopicMetadata buildTopicMetadata(String topic, Node node) {
        List<Node> replicas = Lists.newArrayList(node);
        List<MetadataResponse.PartitionMetadata> partitions = Lists.newArrayList(
                new MetadataResponse.PartitionMetadata(Errors.NONE, 0 /* partition */,
                                                       node /* leader */,
                                                       replicas /* replicas */,
                                                       replicas /* isr */,
                                                       Collections.emptyList() /* offlined replicas */));
        return new MetadataResponse.TopicMetadata(Errors.NONE, topic, false /* isInternal */, partitions);
    }

    private CompletableFuture<MetadataResponse> handleMetadataRequest(ChannelHandlerContext ctx,
                                                                      String namespace,
                                                                      MetadataRequest request) {
        CHashGroup.State chashGroupState = chashGroup.currentState();
        Map<String, CompletableFuture<Node>> nodeFutures = chashGroupState.allOwners().stream()
            .collect(ImmutableMap.toImmutableMap(
                             n -> n,
                             n -> nodeIds.idForNode(n).thenApply((id) -> addressToNode(id, n))));

        CompletableFuture<MetadataResponse> promise = new CompletableFuture<MetadataResponse>();
        CompletableFuture.allOf(nodeFutures.values().toArray(new CompletableFuture[0]))
            .whenComplete((ignore, ignoreException) -> {
                    Map<String, Node> nodes = nodeFutures.entrySet().stream().filter(e -> {
                            try {
                                e.getValue().join();
                                return true;
                            } catch (CompletionException ce) {
                                return false;
                            }
                        }).collect(ImmutableMap.toImmutableMap(e -> e.getKey(),
                                                               e -> e.getValue().join()));
                    handleMetadataRequestWithBrokerList(ctx, namespace, request, chashGroupState, nodes, promise);
                });
        return promise;
    }

    private CompletableFuture<MetadataResponse> handleMetadataRequestWithBrokerList(ChannelHandlerContext ctx,
                                                                                    String namespace,
                                                                                    MetadataRequest request,
                                                                                    CHashGroup.State chashGroupState,
                                                                                    Map<String, Node> nodes,
                                                                                    CompletableFuture<MetadataResponse> promise) {
        List<Node> brokers = nodes.values().stream().collect(Collectors.toList());
        long numTopicsRequested = request.topics() == null ? 0 : request.topics().size();

        if (numTopicsRequested == 0) {
            executor.execute(() -> {
                    try {
                        List<MetadataResponse.TopicMetadata> metadata = admin.topics()
                            .getList(namespace)
                            .stream().map((topic) -> {
                                    TopicName tn = TopicName.get(topic);
                                    Node owner = nodes.get(chashGroupState.lookupOwner(tn.getLookupName()));
                                    return buildTopicMetadata(tn.getLocalName(), owner);
                                })
                            .collect(Collectors.toList());
                        promise.complete(new MetadataResponse(brokers,
                                                              clusterName,
                                                              brokers.stream().findFirst().map(Node::id).orElse(-1),
                                                              metadata));
                    } catch (PulsarAdminException t) {
                        promise.completeExceptionally(t);
                    }
                });
        } else {
            boolean autoCreate = request.allowAutoTopicCreation();
            List<CompletableFuture<MetadataResponse.TopicMetadata>> metadataFutures =
                request.topics().stream().map((topic) -> {

                    try {
                        TopicName topicName = TopicName.get(TopicDomain.persistent.value(),
                                NamespaceName.get(namespace), topic);

                        Node owner = nodes.get(chashGroupState.lookupOwner(topicName.getLookupName()));
                        return admin.topics().getStatsAsync(topicName.toString()).thenApply(new Function<TopicStats,
                                TopicStats>() {

                            @Override
                            public TopicStats apply(TopicStats topicStats) {
                                return topicStats;
                            }
                        })
                                .thenApply((stats) -> buildTopicMetadata(topicName.getLocalName(), owner))
                                .exceptionally((t) -> {
                                    List<MetadataResponse.PartitionMetadata> partitions = Collections.emptyList();
                                    Errors error = pulsarToKafkaException(request, t);
                                    return new MetadataResponse.TopicMetadata(error, topicName.getLocalName(),
                                            false, partitions);
                                })
                                .thenCompose((metadata) -> {
                                    if (metadata.error() == Errors.UNKNOWN_TOPIC_OR_PARTITION
                                            && request.allowAutoTopicCreation()) {
                                        return client.newProducer().topic(topicName.toString()).createAsync()
                                                .thenCompose((producer) -> producer.closeAsync())
                                                .thenApply((ignore) -> buildTopicMetadata(topicName.getLocalName(), owner));
                                    } else {
                                        return CompletableFuture.completedFuture(metadata);
                                    }
                                });
                    } catch (Exception e) {
                        CompletableFuture<MetadataResponse.TopicMetadata> future = new CompletableFuture<>();
                        future.completeExceptionally(e);
                        return future;
                    }
                }).collect(Collectors.toList());

            CompletableFuture.allOf(metadataFutures.toArray(new CompletableFuture[0]))
                .whenComplete((ignore, ex) -> {
                        if (ex != null) {
                            log.warn("Exception fetching metadata", ex);
                            promise.completeExceptionally(ex);
                        } else {
                            List<MetadataResponse.TopicMetadata> metadata =
                                metadataFutures.stream().map(f -> f.join()).collect(Collectors.toList());
                            promise.complete(new MetadataResponse(brokers,
                                                                  clusterName,
                                                                  0, /* controller Id */
                                                                  metadata));
                        }
                    });
        }
        return promise;
    }

    private CompletableFuture<ProduceResponse> handleProduceRequest(ChannelHandlerContext ctx,
                                                                    String namespace,
                                                                    ProduceRequest request) {
        CompletableFuture<ProduceResponse> promise = new CompletableFuture<>();
        if (request.transactionalId() != null) {
            if (log.isDebugEnabled()) {
                log.debug("Transactions not supported by gateway, attempted by client {}", ctx.channel());
            }
            promise.completeExceptionally(new UnsupportedOperationException("No transaction support"));
            return promise;
        }

        if (log.isDebugEnabled()) {
            log.debug("Ignoring produce paramaters, acks({}) & timeout({})",
                      request.acks(), request.timeout());
        }

        Map<TopicPartition, CompletableFuture<?>> responses = new HashMap<>();
        for (Map.Entry<TopicPartition, ? extends Records> e : request.partitionRecordsOrFail().entrySet()) {
            String topic = namespace + "/" + e.getKey().topic();

            List<CompletableFuture<?>> msgFutures = Streams.stream(e.getValue().records())
                .map(r -> producerThread.publish(topic, r)).collect(Collectors.toList());
            responses.put(e.getKey(), CompletableFuture.allOf(msgFutures.toArray(new CompletableFuture<?>[0])));
        }
        CompletableFuture.allOf(responses.values().toArray(new CompletableFuture<?>[responses.size()]))
            .whenComplete((res, ex) -> {
                    Map<TopicPartition, ProduceResponse.PartitionResponse> partitionsResponses = new HashMap<>();
                    for (Map.Entry<TopicPartition, CompletableFuture<?>> e : responses.entrySet()) {
                        Errors errors = Errors.NONE;
                        try {
                            e.getValue().get();
                        } catch (Exception exp) {
                            log.error("Exception processing write to {}", e.getKey(), exp);
                            errors = pulsarToKafkaException(request, exp);
                        }
                        partitionsResponses.put(e.getKey(), new ProduceResponse.PartitionResponse(errors));
                    }
                    promise.complete(new ProduceResponse(partitionsResponses));
                });
        return promise;
    }

    private CompletableFuture<FindCoordinatorResponse> handleFindCoordinator(ChannelHandlerContext ctx,
                                                                             String namespace,
                                                                             FindCoordinatorRequest request) {
        CompletableFuture<FindCoordinatorResponse> promise = new CompletableFuture<>();
        if (request.coordinatorType() == FindCoordinatorRequest.CoordinatorType.GROUP) {
            String key = ConsumerGroups.groupId(namespace, request.coordinatorKey());
            String coordinator = chashGroup.currentState().lookupOwner(key);
            nodeIds.idForNode(coordinator)
                .thenApply(id -> addressToNode(id, coordinator))
                .whenComplete((node, exception) -> {
                        if (exception != null) {
                            promise.completeExceptionally(exception);
                        } else {
                            log.debug("Coordinator for {} is {}", request.coordinatorKey(), node);
                            promise.complete(new FindCoordinatorResponse(Errors.NONE, node));
                        }
                    });
        } else {
            String err = String.format("Unsupported coordinator type %s", request.coordinatorType());
            log.warn(err);
            promise.completeExceptionally(new UnsupportedOperationException(err));
        }

        return promise;
    }

    private CompletableFuture<ListOffsetResponse.PartitionData> getInitialPartitionData(String namespace,
                                                                                        TopicPartition tp) {
        CompletableFuture<ListOffsetResponse.PartitionData> promise = new CompletableFuture<>();
        admin.topics().getInternalStatsAsync(namespace + "/" + tp.topic())
            .whenComplete((result, ex) -> {
                    if (ex != null) {
                        promise.complete(new ListOffsetResponse.PartitionData(Errors.UNKNOWN_SERVER_ERROR,
                                                                              ListOffsetResponse.UNKNOWN_TIMESTAMP,
                                                                              ListOffsetResponse.UNKNOWN_OFFSET));
                    } else {
                        List<PersistentTopicInternalStats.LedgerInfo> ledgers = result.ledgers;
                        long offset = MessageIdUtils.getOffset(ledgers.get(0).ledgerId, 0);
                        promise.complete(new ListOffsetResponse.PartitionData(Errors.NONE,
                                                                              ListOffsetResponse.UNKNOWN_TIMESTAMP,
                                                                              offset));
                    }
                });
        return promise;
    }

    private CompletableFuture<ListOffsetResponse> handleListOffset(ChannelHandlerContext ctx,
                                                                   String namespace,
                                                                   ListOffsetRequest request) {
        CompletableFuture<ListOffsetResponse> promise = new CompletableFuture<>();
        Map<TopicPartition, CompletableFuture<ListOffsetResponse.PartitionData>> offsets =
            request.partitionTimestamps().entrySet().stream()
            .collect(ImmutableMap.toImmutableMap(
                             Map.Entry::getKey,
                             e -> getInitialPartitionData(namespace, e.getKey())));

        CompletableFuture.allOf(offsets.values().toArray(new CompletableFuture[0]))
            .whenComplete((ignore, ex) -> {
                    if (ex != null) {
                        promise.completeExceptionally(ex);
                    } else {
                        Map<TopicPartition, ListOffsetResponse.PartitionData> data =
                            offsets.entrySet().stream().collect(
                                    ImmutableMap.toImmutableMap(
                                            Map.Entry::getKey,
                                            e -> e.getValue().join()));
                        promise.complete(new ListOffsetResponse(data));
                    }
                });
        return promise;
    }

    private CompletableFuture<OffsetFetchResponse> handleOffsetFetch(ChannelHandlerContext ctx,
                                                                     String namespace,
                                                                     OffsetFetchRequest request) {
        CompletableFuture<OffsetFetchResponse> promise = new CompletableFuture<>();
        consumerGroups.getCommittedOffset(namespace, request.groupId(), request.partitions())
            .whenComplete((result, exception) -> {
                    if (exception != null) {
                        promise.completeExceptionally(exception);
                    } else {
                        Map<TopicPartition, OffsetFetchResponse.PartitionData> data = result.entrySet().stream()
                            .collect(ImmutableMap.toImmutableMap(Map.Entry::getKey,
                                             e -> new OffsetFetchResponse.PartitionData(e.getValue().offset(),
                                                                                        e.getValue().metadata(),
                                                                                        Errors.NONE)));
                        promise.complete(new OffsetFetchResponse(Errors.NONE, data));
                    }
                });
        return promise;
    }

    private CompletableFuture<OffsetCommitResponse> handleOffsetCommit(ChannelHandlerContext ctx,
                                                                       String namespace,
                                                                       OffsetCommitRequest request) {
        CompletableFuture<OffsetCommitResponse> promise = new CompletableFuture<>();

        consumerGroups.commitOffset(namespace, request.groupId(), request.memberId(),
                request.generationId(),
                request.offsetData().entrySet().stream()
                .collect(ImmutableMap.toImmutableMap(Map.Entry::getKey,
                                                     e -> new OffsetAndMetadata(e.getValue().offset,
                                                                                e.getValue().metadata))))
            .whenComplete((ignore, exception) -> {
                    if (exception != null) {
                        promise.completeExceptionally(exception);
                    } else {
                        Map<TopicPartition, Errors> result = request.offsetData().entrySet().stream()
                            .collect(ImmutableMap.toImmutableMap(Map.Entry::getKey, e -> Errors.NONE));
                        promise.complete(new OffsetCommitResponse(result));
                    }
                });
        return promise;
    }

    private static MemoryRecords messagesToRecords(List<Message<byte[]>> messages) {
        try (ByteBufferOutputStream os = new ByteBufferOutputStream(DEFAULT_FETCH_BUFFER_SIZE)) {
            long firstOffset = messages.size() > 0 ? MessageIdUtils.getOffset(messages.get(0).getMessageId()) : 0;
            MemoryRecordsBuilder builder = new MemoryRecordsBuilder(os, RecordBatch.CURRENT_MAGIC_VALUE,
                                                                    CompressionType.NONE,
                                                                    TimestampType.CREATE_TIME,
                                                                    firstOffset,
                                                                    RecordBatch.NO_TIMESTAMP,
                                                                    RecordBatch.NO_PRODUCER_ID,
                                                                    RecordBatch.NO_PRODUCER_EPOCH,
                                                                    RecordBatch.NO_SEQUENCE,
                                                                    false, false,
                                                                    RecordBatch.NO_PARTITION_LEADER_EPOCH,
                                                                    MAX_FETCH_BUFFER_SIZE);
            for (Message<byte[]> m : messages) {
                long msgOffset = MessageIdUtils.getOffset(m.getMessageId());
                /** kafka doesn't like offset deltas more than Integer.MAX_VALUE,
                    which can happen quite easily if ledger rolls. If this is the case,
                    only return the messages we have so far. The rest will be returned
                    for the next fetch. */
                if (msgOffset - firstOffset >= Integer.MAX_VALUE) {
                    break;
                }
                builder.appendWithOffset(msgOffset, m.getEventTime(), m.getKeyBytes(), m.getData());
            }
            return builder.build();
        } catch (IOException ioe) {
            throw new UncheckedIOException(ioe);
        }
    }

    private CompletableFuture<FetchResponse> handleFetch(ChannelHandlerContext ctx,
                                                         String namespace,
                                                         FetchRequest request) {
        CompletableFuture<FetchResponse> promise = new CompletableFuture<>();

        Map<TopicPartition, FetchRequest.PartitionData> partitions = request.fetchData();
        Map<TopicPartition, Long> topicsAndOffsets = request.fetchData().entrySet().stream()
            .map((e) -> Pair.of(e.getKey(), e.getValue().fetchOffset))
            .collect(Collectors.toMap(Pair::getLeft, Pair::getRight));

        long timeoutMs = request.maxWait() > 0 ? request.maxWait() : Long.MAX_VALUE;
        Predicate<Integer> hasEnough = (byteCount) -> {
            if (request.maxWait() <= 0) {
                return byteCount >= request.minBytes();
            } else {
                return byteCount >= request.maxBytes();
            }
        };
        fetcher.fetch(namespace, topicsAndOffsets, hasEnough, timeoutMs, TimeUnit.MILLISECONDS)
            .thenApply((results) -> results.entrySet().stream().map((e) -> {
                        Errors error = e.getValue().getError().map((t) -> pulsarToKafkaException(request, t))
                            .orElse(Errors.NONE);
                        MemoryRecords records = messagesToRecords(e.getValue().getMessages());

                        return Pair.of(e.getKey(),
                                       new FetchResponse.PartitionData(error,
                                                                       FetchResponse.INVALID_HIGHWATERMARK,
                                                                       FetchResponse.INVALID_LAST_STABLE_OFFSET,
                                                                       FetchResponse.INVALID_LOG_START_OFFSET,
                                                                       null,
                                                                       records));
                    }).collect(Collectors.toMap(Pair::getLeft, Pair::getRight,
                                                (v1,v2) -> v1, LinkedHashMap::new)))
            .whenComplete((responseData, exception) -> {
                    if (exception != null) {
                        promise.complete(new FetchResponse(pulsarToKafkaException(request, exception),
                                                           new LinkedHashMap<>(),
                                                           ((Integer)THROTTLE_TIME_MS.defaultValue),
                                                           request.metadata().sessionId()));
                    } else {
                        promise.complete(new FetchResponse(Errors.NONE, responseData,
                                                           ((Integer)THROTTLE_TIME_MS.defaultValue),
                                                           request.metadata().sessionId()));
                    }
                });
        return promise;
    }

    private CompletableFuture<JoinGroupResponse> handleJoinGroup(ChannelHandlerContext ctx,
                                                                 String namespace,
                                                                 JoinGroupRequest request) {
        String memberId = !Strings.isNullOrEmpty(request.memberId()) ? request.memberId() : UUID.randomUUID().toString();
        ImmutableMap<String, ByteString> protocols =
            request.groupProtocols().stream().collect(
                    ImmutableMap.toImmutableMap(e -> e.name(),
                                                e -> ByteString.copyFrom(e.metadata())));
        return consumerGroups.joinGroup(namespace, request.groupId(), memberId,
                                        request.sessionTimeout(), request.rebalanceTimeout(),
                                        protocols)
            .thenApply((state) -> {
                    Map<String, ByteBuffer> metadata = state.getMemberProtocolMetadata()
                        .entrySet().stream().collect(
                                ImmutableMap.toImmutableMap(e -> e.getKey(),
                                                            e -> e.getValue().asReadOnlyByteBuffer()));
                    return new JoinGroupResponse(Errors.NONE, state.getGeneration(),
                                                 state.getCurrentProtocol(),
                                                 memberId, state.getLeader(),
                                                 metadata);
                });
    }

    private CompletableFuture<SyncGroupResponse> handleSyncGroup(ChannelHandlerContext ctx,
                                                                 String namespace,
                                                                 SyncGroupRequest request) {
        // The ByteBuffer lives past the request, so it must be duplicated
        ImmutableMap<String, ByteString> assignments = request.groupAssignment().entrySet()
            .stream().collect(ImmutableMap.toImmutableMap(e -> e.getKey(),
                                                          e -> ByteString.copyFrom(e.getValue())));
        return consumerGroups.syncGroup(namespace, request.groupId(), request.memberId(),
                                        request.generationId(),
                                        assignments)
            .thenApply((assignment) -> new SyncGroupResponse(Errors.NONE, assignment.asReadOnlyByteBuffer()));
    }

    private CompletableFuture<HeartbeatResponse> handleHeartbeart(ChannelHandlerContext ctx,
                                                                  String namespace,
                                                                  HeartbeatRequest request) {
        return consumerGroups.heartbeat(namespace, request.groupId(), request.memberId(),
                                        request.groupGenerationId())
            .thenApply((ignore) -> new HeartbeatResponse(Errors.NONE));
    }

    private CompletableFuture<SaslHandshakeResponse> handleSaslHandshakeRequest(ChannelHandlerContext ctx,
                                                                                SaslHandshakeRequest request) {
        CompletableFuture<SaslHandshakeResponse> response = new CompletableFuture<>();
        if (Strings.nullToEmpty(request.mechanism()).equals("PLAIN")) {
            response.complete(new SaslHandshakeResponse(Errors.NONE, Lists.newArrayList("PLAIN")));
        } else {
            response.complete(new SaslHandshakeResponse(Errors.UNSUPPORTED_SASL_MECHANISM,
                                                        Lists.newArrayList("PLAIN")));
        }
        return response;
    }

    private CompletableFuture<SaslAuthenticateResponse> handleSaslAuthenticateRequest(ChannelHandlerContext ctx,
                                                                                      SaslAuthenticateRequest request) {
        CompletableFuture<SaslAuthenticateResponse> response = new CompletableFuture<>();
        ByteBuffer buf = request.saslAuthBytes();
        byte[] bytes = new byte[buf.remaining()];
        buf.get(bytes);

        String[] parts = new String(bytes, UTF_8).split("\u0000");
        if (parts.length != 3) {
            response.complete(new SaslAuthenticateResponse(
                                      Errors.SASL_AUTHENTICATION_FAILED,
                                      "Invalid login data"));
        } else if (parts[1].isEmpty()) {
            response.complete(new SaslAuthenticateResponse(
                                      Errors.SASL_AUTHENTICATION_FAILED,
                                      "Namespace must be specified as username"));
        } else if (parts[2].isEmpty()) {
            response.complete(new SaslAuthenticateResponse(
                                      Errors.SASL_AUTHENTICATION_FAILED,
                                      "Token must be specified as password"));
        } else {
            String namespace = parts[1];
            String token = parts[2];
            try {
                AuthenticationDataSource dataSource = new AuthenticationDataSource() {
                        @Override
                        public boolean hasDataFromCommand() {
                            return true;
                        }
                        @Override
                        public String getCommandData() {
                            return token;
                        }
                    };

                // Assume token auth for now
                String user = null;
                if (config.isAuthenticationEnabled()) {
                    user = authenticationService.authenticate(dataSource, "token");
                }
                TopicName topicName = TopicName.get(TopicDomain.persistent.value(),
                        NamespaceName.get(namespace), "dummy-topic");

                final String finalUser = user;
                authzService.map((service) -> {
                        CompletableFuture<Boolean> canProduce = service.canProduceAsync(
                                topicName, finalUser, dataSource);
                        CompletableFuture<Boolean> canConsume = service.canConsumeAsync(
                                topicName, finalUser, dataSource, "kafka-sub");
                        return CompletableFuture.allOf(canProduce, canConsume)
                            .thenApply((ignore) -> {
                                    return canProduce.join() && canConsume.join();
                                });
                    })
                    .orElseGet(() -> CompletableFuture.completedFuture(true))
                    .whenComplete((authorized, exception) -> {
                            if (exception != null) {
                                response.complete(new SaslAuthenticateResponse(
                                                          Errors.SASL_AUTHENTICATION_FAILED,
                                                          exception.getMessage()));
                            } else if (!authorized) {
                                response.complete(new SaslAuthenticateResponse(
                                                          Errors.SASL_AUTHENTICATION_FAILED,
                                                          "Not authorized for namespace"));
                            } else {
                                ctx.attr(AUTH_ATTR_KEY).set(new AuthenticatedUser(namespace, finalUser));
                                response.complete(new SaslAuthenticateResponse(Errors.NONE, "", ByteBuffer.allocate(0)));
                            }
                        });
            } catch (AuthenticationException e) {
                response.complete(new SaslAuthenticateResponse(
                                          Errors.SASL_AUTHENTICATION_FAILED,
                                          e.getMessage()));
            }
        }
        return response;
    }

    CompletableFuture<String> checkAuth(ChannelHandlerContext ctx) {
        CompletableFuture<String> promise = new CompletableFuture<>();
        if (config.isAuthenticationEnabled()) {
            if (ctx.attr(AUTH_ATTR_KEY).get() != null) {
                promise.complete(ctx.attr(AUTH_ATTR_KEY).get().namespace);
            } else {
                promise.completeExceptionally(new AuthorizationException("Connection not authorized"));
            }
        } else {
            // Even if authentication is turned of users can still specify namespace
            if (ctx.attr(AUTH_ATTR_KEY).get() != null) {
                promise.complete(ctx.attr(AUTH_ATTR_KEY).get().namespace);
            } else {
                // Use the default namespace
                promise.complete(config.getKafkaPulsarDefaultNamespace());
            }
        }

        return promise;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        if (log.isDebugEnabled()) {
            log.debug("Got message in handler {}", msg);
        }
        if (msg instanceof KafkaProtocolCodec.KafkaHeaderAndRequest) {
            KafkaProtocolCodec.KafkaHeaderAndRequest har = (KafkaProtocolCodec.KafkaHeaderAndRequest)msg;
            ReferenceCountUtil.retain(har);

            CompletableFuture<? extends AbstractResponse> responsePromise;
            switch (har.getHeader().apiKey()) {
            case API_VERSIONS:
                responsePromise = CompletableFuture.completedFuture(
                                                                    ApiVersionsResponse.defaultApiVersionsResponse());
                break;
            case SASL_HANDSHAKE:
                responsePromise = handleSaslHandshakeRequest(ctx, (SaslHandshakeRequest)har.getRequest());
                break;
            case SASL_AUTHENTICATE:
                responsePromise = handleSaslAuthenticateRequest(ctx, (SaslAuthenticateRequest)har.getRequest());
                break;
            case METADATA:
                responsePromise = checkAuth(ctx)
                    .thenCompose(namespace -> handleMetadataRequest(
                                         ctx, namespace, (MetadataRequest)har.getRequest()));
                break;
            case PRODUCE:
                responsePromise = checkAuth(ctx)
                    .thenCompose(namespace -> handleProduceRequest(
                                         ctx, namespace, (ProduceRequest)har.getRequest()));
                break;
            case FIND_COORDINATOR:
                responsePromise = checkAuth(ctx)
                    .thenCompose(namespace -> handleFindCoordinator(
                                         ctx, namespace, (FindCoordinatorRequest)har.getRequest()));
                break;
            case LIST_OFFSETS:
                responsePromise = checkAuth(ctx)
                    .thenCompose(namespace -> handleListOffset(
                                         ctx, namespace, (ListOffsetRequest)har.getRequest()));
                break;
            case OFFSET_FETCH:
                responsePromise = checkAuth(ctx)
                    .thenCompose(namespace -> handleOffsetFetch(
                                         ctx, namespace, (OffsetFetchRequest)har.getRequest()));
                break;
            case OFFSET_COMMIT:
                responsePromise = checkAuth(ctx)
                    .thenCompose(namespace -> handleOffsetCommit(
                                         ctx, namespace, (OffsetCommitRequest)har.getRequest()));
                break;
            case FETCH:
                responsePromise = checkAuth(ctx)
                    .thenCompose(namespace -> handleFetch(
                                         ctx, namespace, (FetchRequest)har.getRequest()));
                break;
            case JOIN_GROUP:
                responsePromise = checkAuth(ctx)
                    .thenCompose(namespace -> handleJoinGroup(
                                         ctx, namespace, (JoinGroupRequest)har.getRequest()));
                break;
            case SYNC_GROUP:
                responsePromise = checkAuth(ctx)
                    .thenCompose(namespace -> handleSyncGroup(
                                         ctx, namespace, (SyncGroupRequest)har.getRequest()));
                break;
            case HEARTBEAT:
                responsePromise = checkAuth(ctx)
                    .thenCompose(namespace -> handleHeartbeart(
                                         ctx, namespace, (HeartbeatRequest)har.getRequest()));
                break;
            default:
                String err = String.format("API_KEY(%s) Not supported by server",
                                           har.getHeader().apiKey());
                log.warn(err);
                if (log.isDebugEnabled()) {
                    log.debug("Unknown message: {}", har.getRequest());
                }
                responsePromise = new CompletableFuture<>();
                responsePromise.completeExceptionally(new UnsupportedOperationException(err));
            }
            Channel channel = ctx.channel();
            completions.compute(channel,
                                (key, queue) -> {
                                    if (queue == null) {
                                        Queue<Completion> newQueue = new LinkedList<>();
                                        newQueue.add(new Completion(har, responsePromise));
                                        return newQueue;
                                    } else {
                                        queue.add(new Completion(har, responsePromise));
                                        return queue;
                                    }
                                });
            responsePromise.whenCompleteAsync((resp, ex) -> sendCompletedResponses(channel), executor);
        }
        ReferenceCountUtil.release(msg);
    }

    private static class Completion {
        private final KafkaProtocolCodec.KafkaHeaderAndRequest har;
        private final CompletableFuture<? extends AbstractResponse> promise;

        Completion(KafkaProtocolCodec.KafkaHeaderAndRequest har, CompletableFuture<? extends AbstractResponse> promise) {
            this.har = har;
            this.promise = promise;
        }

        KafkaProtocolCodec.KafkaHeaderAndRequest getHeaderAndRequest() {
            return har;
        }

        CompletableFuture<? extends AbstractResponse> getPromise() {
            return promise;
        }
    }

    private void sendCompletedResponses(Channel channel) {
        completions.compute(channel,
                            (key, queue) -> {
                                while (queue != null
                                       && queue.peek() != null
                                       && queue.peek().getPromise().isDone()) {
                                    Completion completion = queue.remove();
                                    AbstractResponse toSend;
                                    try {
                                        toSend = completion.getPromise().join();
                                    } catch (CompletionException ex) {
                                        toSend = completion.getHeaderAndRequest().getRequest()
                                            .getErrorResponse(((Integer)THROTTLE_TIME_MS.defaultValue),
                                                              stripException(ex));
                                    }

                                    KafkaProtocolCodec.KafkaHeaderAndRequest request = completion.getHeaderAndRequest();
                                    KafkaProtocolCodec.KafkaHeaderAndResponse response =
                                        KafkaProtocolCodec.KafkaHeaderAndResponse.responseForRequest(request, toSend);
                                    ReferenceCountUtil.release(request);

                                    if (log.isDebugEnabled()) {
                                        log.debug("Responding with {}", response);
                                    }
                                    channel.writeAndFlush(response);
                                }
                                if (queue == null || queue.isEmpty()) {
                                    return null;
                                } else {
                                    return queue;
                                }
                            });
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.error("Caught error in handler, closing channel", cause);
        ctx.close();
    }

    private static Throwable stripException(Throwable t) {
        if (t instanceof CompletionException || t instanceof ExecutionException) {
            return stripException(t.getCause());
        } else {
            return t;
        }
    }
    private static Errors pulsarToKafkaException(AbstractRequest request, Throwable exception) {
        if (log.isDebugEnabled()) {
            log.debug("Converting error for request {}", request, exception);
        }
        if (exception instanceof PulsarAdminException.NotFoundException) {
            return Errors.UNKNOWN_TOPIC_OR_PARTITION;
        } else if (exception.getCause() != null) {
            return pulsarToKafkaException(request, exception.getCause());
        } else {
            return Errors.UNKNOWN_SERVER_ERROR;
        }
    }

    static class AuthenticatedUser {
        final String namespace;
        final String user;

        AuthenticatedUser(String namespace, String user) {
            this.namespace = namespace;
            this.user = user;
        }
    }

    final static AttributeKey<AuthenticatedUser> AUTH_ATTR_KEY = AttributeKey.valueOf("AuthUser");
}
