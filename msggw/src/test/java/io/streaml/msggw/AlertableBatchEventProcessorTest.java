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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.LifecycleAware;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.Sequence;
import com.lmax.disruptor.SequenceBarrier;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.util.DaemonThreadFactory;

import io.streaml.msggw.AlertableBatchEventProcessor.AlertHandler;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.junit.Test;

public class AlertableBatchEventProcessorTest {

    private final static Logger log = LoggerFactory.getLogger(AlertableBatchEventProcessorTest.class);

    @Test
    public void alertWakesUpThread() throws Exception {
        Disruptor<TestEvent> disruptor = new Disruptor<>(TestEvent::new, 1024,
                                                         DaemonThreadFactory.INSTANCE);
        RingBuffer<TestEvent> ringBuffer = disruptor.getRingBuffer();
        SequenceBarrier barrier = ringBuffer.newBarrier(new Sequence[0]);

        Handler handler = new Handler();
        AlertableBatchEventProcessor processor = new AlertableBatchEventProcessor(ringBuffer, barrier, handler);
        disruptor.handleEventsWith(processor);
        disruptor.start();
        handler.awaitStarted();

        log.info("First alert");
        barrier.alert();

        log.info("Ensure that alert occurred");
        handler.alertThread.get();

        log.info("Sending event");
        disruptor.publishEvent((event, sequence, value) -> event.setValue(value), 123);

        log.info("Ensure that event was processed");
        handler.eventThread.get();

        assertThat(handler.eventThread.get(), is(handler.alertThread.get()));

        log.info("Second alert");
        barrier.alert();

        for (int i = 0; i < 10 && handler.numAlerts.get() < 2; i++) {
            Thread.sleep(100);
        }

        assertThat(handler.numAlerts.get(), is(2));

        disruptor.halt();
        disruptor.shutdown();
    }


    static class Handler implements EventHandler<TestEvent>, AlertHandler, LifecycleAware {
        final CompletableFuture<Void> start = new CompletableFuture<>();
        final CompletableFuture<Thread> eventThread = new CompletableFuture<>();
        final CompletableFuture<Thread> alertThread = new CompletableFuture<>();
        final AtomicInteger numAlerts = new AtomicInteger(0);

        public void awaitStarted() throws Exception {
            start.get();
        }

        @Override
        public void onStart() {
            start.complete(null);
        }

        @Override
        public void onShutdown() {}

        @Override
        public void onEvent(TestEvent event, long sequence, boolean endOfBatch) {
            log.info("Event triggered");
            eventThread.complete(Thread.currentThread());
        }

        @Override
        public void onAlert(long sequence) {
            log.info("Alert triggered");
            numAlerts.incrementAndGet();
            alertThread.complete(Thread.currentThread());
        }
    }

    private static class TestEvent {
        int i;

        TestEvent() {
            i = 0;
        }

        void setValue(int newVal) {
            this.i = newVal;
        }
    }
}
