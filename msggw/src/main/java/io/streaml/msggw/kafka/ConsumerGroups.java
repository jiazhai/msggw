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
import com.google.protobuf.ByteString;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;

public interface ConsumerGroups {
    static class MemberData {
        final int sessionTimeout;
        final int rebalanceTimeout;
        final ImmutableMap<String, ByteString> protocolMetadata;

        MemberData(int sessionTimeout, int rebalanceTimeout,
                   ImmutableMap<String, ByteString> protocolMetadata) {
            this.sessionTimeout = sessionTimeout;
            this.rebalanceTimeout = rebalanceTimeout;
            this.protocolMetadata = protocolMetadata;
        }

        int getSessionTimeout() {
            return sessionTimeout;
        }

        int getRebalanceTimeout() {
            return rebalanceTimeout;
        }

        ImmutableMap<String, ByteString> getProtocolMetadata() {
            return protocolMetadata;
        }
    }

    interface State {
        String getGroupName();
        int getGeneration();
        String getCurrentProtocol();
        String getLeader();
        ImmutableMap<String, ByteString> getMemberProtocolMetadata();
        ImmutableMap<String, MemberData> getMemberData();

        CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> getOffsetAndMetadata(Collection<TopicPartition> topics);
        CompletableFuture<Void> putOffsetAndMetadata(int generation, Map<TopicPartition, OffsetAndMetadata> offsets);
        ConsumerGroupStorage.Handle getStorageHandle();

        CompletableFuture<State> close();
    }

    CompletableFuture<State> joinGroup(String namespace, String group, String name,
                                       int sessionTimeout, int rebalanceTimeout,
                                       ImmutableMap<String, ByteString> protocols);

    CompletableFuture<State> leaveGroup(String namespace, String group, String name);

    CompletableFuture<ByteString> syncGroup(String namespace, String group, String name, int generation,
                                            ImmutableMap<String, ByteString> assignments);

    CompletableFuture<Void> heartbeat(String namespace, String group, String name, int generation);

    CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> getCommittedOffset(
            String namespace, String group, List<TopicPartition> topics);
    CompletableFuture<Void> commitOffset(String namepace, String group, String name,
            int generation, Map<TopicPartition, OffsetAndMetadata> offsets);
    CompletableFuture<Void> processTimeouts();

    static String groupId(String namespace, String group) {
        return namespace.replace("/", ".") + "." + group;
    }
}
