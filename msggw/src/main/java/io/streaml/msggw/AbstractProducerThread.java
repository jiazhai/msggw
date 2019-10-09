package io.streaml.msggw;

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

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.Ticker;

import com.google.common.util.concurrent.AbstractService;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TypedMessageBuilder;

import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.LifecycleAware;
import com.lmax.disruptor.PhasedBackoffWaitStrategy;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.Sequence;
import com.lmax.disruptor.SequenceBarrier;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import com.lmax.disruptor.util.Util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractProducerThread<T> extends AbstractService implements ProducerThread<T> {
    private static final Logger log = LoggerFactory.getLogger(AbstractProducerThread.class);

    private final Disruptor<MessageEvent> disruptor;
    private final RingBuffer<MessageEvent> ringBuffer;
    private final SequenceBarrier barrier;
    private final PulsarClient pulsarClient;
    private final Ticker ticker;
    private final int maxPending;
    private final int maxCachedProducers;
    private final int cachedProducerExpirySecs;

    public AbstractProducerThread(PulsarClient pulsarClient,
                                  String threadNameFormat,
                                  int queueSize,
                                  int maxPending,
                                  Ticker ticker,
                                  int maxCachedProducers,
                                  int cachedProducerExpirySecs) {
        this.pulsarClient = pulsarClient;
        this.ticker = ticker;
        this.maxPending = maxPending;
        this.maxCachedProducers = maxCachedProducers;
        this.cachedProducerExpirySecs = cachedProducerExpirySecs;

        disruptor = new Disruptor<>(MessageEvent::new,
                                    Util.ceilingNextPowerOfTwo(queueSize),
                                    new ThreadFactoryBuilder().setDaemon(true).setNameFormat(threadNameFormat).build(),
                                    ProducerType.MULTI,
                                    new PhasedBackoffWaitStrategy(100, 200, TimeUnit.MILLISECONDS,
                                                                  new BlockingWaitStrategy()));
        ringBuffer = disruptor.getRingBuffer();
        barrier = ringBuffer.newBarrier(new Sequence[0]);
        disruptor.handleEventsWith(new AlertableBatchEventProcessor(ringBuffer, barrier, new Handler()));
    }

    static class PendingEvents {
        final CompletableFuture<Producer<byte[]>> producerPromise;
        final List<MessageEvent> messages = new ArrayList<>();

        PendingEvents(CompletableFuture<Producer<byte[]>> producerPromise) {
            this.producerPromise = producerPromise;
        }

        void addMessage(MessageEvent message) {
            messages.add(message);
        }

        CompletableFuture<Producer<byte[]>> getProducerPromise() {
            return producerPromise;
        }

        List<MessageEvent> getMessages() {
            return messages;
        }
    }

    
    class Handler implements EventHandler<MessageEvent>, AlertableBatchEventProcessor.AlertHandler, LifecycleAware {
        final Cache<String, Producer<byte[]>> producerCache;
        final Map<String, PendingEvents> pendingEvents;
        int numPending;

        Handler() {
            numPending = 0;
            pendingEvents = new HashMap<>();
            producerCache = Caffeine.newBuilder()
                .expireAfterAccess(cachedProducerExpirySecs, TimeUnit.SECONDS)
                .maximumSize(maxCachedProducers)
                .ticker(ticker)
                .removalListener((String key, Producer<byte[]> producer, RemovalCause cause) -> {
                        if (log.isDebugEnabled()) {
                            log.debug("Evicting producer for key({}) for reason({})", key, cause);
                        }
                        producer.closeAsync().whenComplete((ignore, ex) -> {
                                if (ex != null) {
                                    log.warn("Error closing producer(topic:{})", producer.getTopic(), ex);
                                }
                            });
                    }).build();
        }

        @Override
        public void onStart() {
            notifyStarted();
        }

        @Override
        public void onShutdown() {
            log.info("Abstract producer thread shutting down. numPending={}", numPending);
            producerCache.invalidateAll();
            notifyStopped();
        }

        @Override
        public void onAlert(long sequenceId) {
            if (log.isDebugEnabled()) {
                log.debug("Handler alerted, try to clear pending messages");
            }
            tryClearPending();
        }

        @Override
        public void onEvent(MessageEvent event, long sequence, boolean endOfBatch) throws Exception {
            tryClearPending();

            while (numPending >= maxPending) {
                if (log.isDebugEnabled()) {
                    log.debug("Max number of messages pending, blocking until some are published");
                }
                try {
                    CompletableFuture.anyOf(pendingEvents.values().stream()
                                            .map(PendingEvents::getProducerPromise)
                            .toArray(CompletableFuture[]::new)).join();
                } catch (CompletionException e) {
                    // ignore, will be picked up while clearing pending
                }
                tryClearPending();
            }

            Producer<byte[]> producer = producerCache.getIfPresent(event.topic);
            if (producer == null) {
                if (log.isDebugEnabled()) {
                    log.debug("[{}] No producer in cache, is pending? {}",
                              event.topic, pendingEvents.containsKey(event.topic));
                }

                pendingEvents.computeIfAbsent(event.topic,
                                              (topic) -> {
                                                  if (log.isDebugEnabled()) {
                                                      log.debug("Creating new producer for topic {}", event.topic);
                                                  }
                                                  CompletableFuture<Producer<byte[]>> promise = createProducer(topic);
                                                  promise.whenComplete((p, ex) -> {
                                                          if (ex != null) {
                                                              log.warn("[{}] Producer creation failed", topic, ex);
                                                          } else if (log.isDebugEnabled()) {
                                                              log.debug("[{}] Producer creation succeeded", topic);
                                                          }

                                                          alert();
                                                      });
                                                  return new PendingEvents(promise);
                                              })
                    .addMessage(event.clone());
                numPending++;
            } else {
                sendMessageEvent(producer, event);
            }
        }

        private void tryClearPending() {
            List<String> completed = pendingEvents.entrySet().stream()
                .filter(e -> e.getValue().getProducerPromise().isDone())
                .map(Map.Entry::getKey).collect(Collectors.toList());
            if (log.isDebugEnabled()) {
                log.debug("Completed producer creations {}", completed);
            }
            completed.forEach((topic) -> {
                    PendingEvents events = pendingEvents.remove(topic);
                    List<MessageEvent> messages = events.getMessages();
                    numPending -= messages.size();
                    try {
                        Producer<byte[]> producer = events.getProducerPromise().join();
                        producerCache.put(topic, producer);

                        events.getMessages().forEach(m -> sendMessageEvent(producer, m));
                    } catch (CompletionException ex) {
                        events.getMessages().forEach(m -> m.promise.completeExceptionally(ex));
                    }
                });
        }

        private void sendMessageEvent(Producer<byte[]> producer, MessageEvent event) {
            TypedMessageBuilder<byte[]> builder = producer.newMessage();
            if (event.key != null) {
                builder.key(event.key);
            } else if (event.keyBytes != null) {
                builder.keyBytes(event.keyBytes);
            }
            if (event.value != null) {
                builder.value(event.value);
            } else {
                builder.value(new byte[0]);
            }
            if (event.sequenceId >= 0) {
                builder.sequenceId(event.sequenceId);
            }
            if (event.eventTime >= 0) {
                builder.eventTime(event.eventTime);
            }
            event.properties.entrySet().forEach(e -> builder.property(e.getKey(), e.getValue()));
            builder.sendAsync().whenComplete((msgId, ex) -> {
                    if (ex != null) {
                        event.promise.completeExceptionally(ex);
                    } else {
                        event.promise.complete(msgId);
                    }
                });
        }

    }

    private void alert() {
        barrier.alert();
    }

    @Override
    protected void doStart() {
        disruptor.start();
    }


    @Override
    protected void doStop() {
        disruptor.halt();
        disruptor.shutdown();
    }

    public abstract void translateMessage(T message, MessageEvent event);

    @Override
    public CompletableFuture<MessageId> publish(String topic, T message) {
        CompletableFuture<MessageId> promise = new CompletableFuture<>();
        ringBuffer.publishEvent((event, sequence, messageValue) -> {
                event.clear();
                event.promise = promise;
                event.topic = topic;

                translateMessage(message, event);
            }, message);
        return promise;
    }

    protected CompletableFuture<Producer<byte[]>> createProducer(String topic) {
        return pulsarClient.newProducer(Schema.AUTO_PRODUCE_BYTES()).topic(topic).createAsync();
    }

    public static class MessageEvent {
        CompletableFuture<MessageId> promise;
        String topic;
        public String key;
        public byte[] keyBytes;
        public byte[] value;
        public long sequenceId;
        public long eventTime;
        public final Map<String, String> properties = new HashMap<>();

        void clear() {
            promise = null;
            topic = null;
            key = null;
            keyBytes = null;
            value = null;
            sequenceId = -1;
            eventTime = -1;
            properties.clear();
        }

        protected MessageEvent clone() {
            MessageEvent newEvent = new MessageEvent();
            newEvent.promise = promise;
            newEvent.topic = topic;
            newEvent.key = key;
            newEvent.keyBytes = keyBytes != null ? Arrays.copyOf(keyBytes, keyBytes.length) : null;
            newEvent.value = value != null ? Arrays.copyOf(value, value.length) : null;
            newEvent.sequenceId = sequenceId;
            newEvent.eventTime = eventTime;
            newEvent.properties.putAll(properties);
            return newEvent;
        }
    }

}
