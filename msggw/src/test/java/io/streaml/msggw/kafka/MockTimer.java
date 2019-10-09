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
import io.netty.util.TimerTask;

import java.util.Comparator;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Mock implementation of netty's Timer interface
 */
class MockTimer implements Timer {
    private final static Logger log = LoggerFactory.getLogger(MockTimer.class);
    Set<MockTimeout> timeouts = new TreeSet<>(Comparator.comparing((t) -> t.getDeadlineNanos()));

    long currentNanos = 0L;

    @Override
    public Timeout newTimeout(TimerTask task,
                              long delay,
                              TimeUnit unit) {
        long deadline = currentNanos + unit.toNanos(delay);

        MockTimeout t = new MockTimeout(task, deadline);
        synchronized (timeouts) {
            timeouts.add(t);
        }
        return t;
    }

    @Override
    public synchronized Set<Timeout> stop() {
        Set<Timeout> toReturn = timeouts.stream().collect(Collectors.toSet());
        timeouts.clear();
        return toReturn;
    }

    void advanceClock(long by, TimeUnit unit) {
        currentNanos += unit.toNanos(by);

        Set<MockTimeout> toExpire;
        synchronized (timeouts) {
            toExpire = timeouts.stream().filter((t) -> t.shouldExpire()).collect(Collectors.toSet());
            timeouts.removeAll(toExpire);
        }
        toExpire.forEach(t -> t.expire());
    }

    private void removeTimeout(MockTimeout timeout) {
        synchronized (timeouts) {
            timeouts.remove(timeout);
        }
    }

    enum TimeoutState { WAITING, CANCELLED, EXPIRED };


    class MockTimeout implements Timeout {
        private final TimerTask task;
        private final long deadlineNanos;
        private TimeoutState state = TimeoutState.WAITING;

        MockTimeout(TimerTask task, long deadlineNanos) {
            this.task = task;
            this.deadlineNanos = deadlineNanos;
        }

        private long getDeadlineNanos() {
            return deadlineNanos;
        }

        @Override
        public Timer timer() {
            return MockTimer.this;
        }

        @Override
        public TimerTask task() {
            return task;
        }

        @Override
        public synchronized boolean isExpired() {
            return state == TimeoutState.EXPIRED;
        }

        @Override
        public synchronized boolean isCancelled() {
            return state == TimeoutState.CANCELLED;
        }

        @Override
        public boolean cancel() {
            synchronized (MockTimer.this) {
                removeTimeout(this);
            }
            synchronized (this) {
                if (state == TimeoutState.WAITING) {
                    state = TimeoutState.CANCELLED;
                    return true;
                } else {
                    return false;
                }
            }
        }

        boolean shouldExpire() {
            return currentNanos >= deadlineNanos;
        }

        synchronized void expire() {
            if (state == TimeoutState.WAITING) {
                try {
                    task.run(this);
                } catch (Exception e) {
                    log.error("Timer task {} threw exception on expiration", task, e);
                }
                state = TimeoutState.EXPIRED;
            }
        }
    }
}
