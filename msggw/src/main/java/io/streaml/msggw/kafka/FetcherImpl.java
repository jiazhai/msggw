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

import io.netty.util.Timeout;
import io.netty.util.Timer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Stream;

import org.apache.kafka.common.TopicPartition;

import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Reader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FetcherImpl implements Fetcher {
    private static final Logger log = LoggerFactory.getLogger(FetcherImpl.class);

    final PulsarClient client;
    final Timer timer;

    public FetcherImpl(PulsarClient client, Timer timer) {
        this.client = client;
        this.timer = timer;
    }

    @Override
    public CompletableFuture<Map<TopicPartition, ? extends Result>> fetch(String namespace,
                                                                          Map<TopicPartition, Long> topicsAndOffsets,
                                                                          Predicate<Integer> haveEnoughBytes,
                                                                          long timeout, TimeUnit timeoutUnit) {
        FetchContext ctx = new FetchContext(topicsAndOffsets.keySet(), haveEnoughBytes, timer, timeout, timeoutUnit);
        topicsAndOffsets.entrySet().stream().forEach((e) -> {
                TopicPartition topic = e.getKey();
                MessageId startMessageId;
                if (e.getValue() <= 0) {
                    startMessageId = MessageId.earliest;
                } else {
                    startMessageId = MessageIdUtils.getMessageId(e.getValue());
                }
                if (log.isDebugEnabled()) {
                    log.debug("[topic:{}] Opening reader at messageId {}", topic, startMessageId);
                }
                // We should start caching the readers later. This can be tricky though, as we need to know that
                // the previous message in reader is startMessageId and I expect this will be tricky with loads
                // of corner cases around ledger rolling.
                String pulsarTopic = namespace + "/" + topic.topic();
                client.newReader().topic(pulsarTopic).startMessageId(startMessageId)
                    .startMessageIdInclusive().createAsync()
                    .whenComplete((reader, exception) -> {
                            if (exception != null) {
                                log.warn("[topic:{}] Error opening reader", topic, exception);
                                ctx.error(topic, exception);
                            } else {
                                readOneLoop(topic, reader, ctx);
                            }
                            ctx.getPromise().whenComplete((ignore, ignoreException) -> {
                                    closeReader(topic, reader);
                                });
                        });

            });
        return ctx.getPromise();
    }

    private static void closeReader(TopicPartition topic, Reader<byte[]> reader) {
        if (log.isDebugEnabled()) {
            log.debug("[topic:{}] Closing reader {}", topic, reader);
        }
        reader.closeAsync().whenComplete((ignore, exception) -> {
                if (exception != null) {
                    log.warn("[topic:{}] Error closing reader {}", topic, reader, exception);
                }
            });
    }

    private void readOneLoop(TopicPartition topic, Reader<byte[]> reader, FetchContext ctx) {
        reader.readNextAsync().whenComplete((message, exception) -> {
                if (exception != null && !ctx.isComplete()) {
                    if (log.isDebugEnabled()) {
                        log.debug("[topic:{}] Error reading message", topic, exception);
                    }
                    ctx.error(topic, exception);
                } else if (ctx.offerMessage(topic, message)
                           && !ctx.isComplete()) {
                    readOneLoop(topic, reader, ctx);
                }
            });
    }

    static class FetchContext {
        static class Data implements Fetcher.Result {
            Throwable exception = null;
            final List<Message<byte[]>> messages = new ArrayList<>();

            void addMessage(Message<byte[]> m) {
                if (!isError()) {
                    messages.add(m);
                }
            }

            int partitionBytes() {
                return messages.stream().map(m -> m.getData().length).reduce(0, (x, y) -> x + y);
            }

            void error(Throwable e) {
                exception = e;
                messages.clear();
            }

            boolean isError() {
                return exception != null;
            }

            @Override
            public Optional<Throwable> getError() {
                return Optional.ofNullable(exception);
            }

            @Override
            public List<Message<byte[]>> getMessages() {
                return messages;
            }

            @Override
            public String toString() {
                return String.format("Data[error:%s, numMessages:%d]", exception, messages.size());
            }
        }

        final Map<TopicPartition, Data> data = new HashMap<>();
        final CompletableFuture<Map<TopicPartition, ? extends Fetcher.Result>> promise = new CompletableFuture<>();

        final Predicate<Integer> haveEnoughBytes;
        final Timeout timeout;

        FetchContext(Set<TopicPartition> topics, Predicate<Integer> haveEnoughBytes,
                     Timer timer, long timeoutDuration, TimeUnit timeoutUnit) {
            topics.forEach((t) -> {
                    data.computeIfAbsent(t, k -> new Data());
                });
            this.haveEnoughBytes = haveEnoughBytes;
            if (timeoutDuration > 0) {
                this.timeout = timer.newTimeout((ignore) -> {
                        if (log.isDebugEnabled()) {
                            log.debug("Timed out fetch for topics {} after {}ms",
                                      topics, timeoutUnit.toMillis(timeoutDuration));
                        }
                        complete();
                    }, timeoutDuration, timeoutUnit);
            } else {
                this.timeout = null;
            }
        }

        synchronized void error(TopicPartition topic, Throwable e) {
            if (!isComplete()) {
                data.computeIfAbsent(topic, k -> new Data()).error(e);
            }
            maybeComplete();
        }

        synchronized boolean offerMessage(TopicPartition topic, Message<byte[]> m) {
            if (isComplete()) {
                return false;
            } else {
                data.computeIfAbsent(topic, k -> new Data()).addMessage(m);
                maybeComplete();
                return true;
            }
        }

        boolean isComplete() {
            return promise.isDone();
        }

        private synchronized void complete() {
            promise.complete(data);
            if (timeout != null) {
                timeout.cancel();
            }
        }

        private void maybeComplete() {
            int totalBytes = data.values().stream().map(partition -> partition.partitionBytes())
                .reduce(0, (sum, partitionBytes) -> sum + partitionBytes);
            boolean allErrors = data.values().stream().allMatch(partition -> partition.isError());

            if (haveEnoughBytes.test(totalBytes) || allErrors) {
                complete();
            }
        }

        CompletableFuture<Map<TopicPartition, ? extends Fetcher.Result>> getPromise() {
            return promise;
        }
    }
}
