/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.simulator.common;

import com.hazelcast.util.QuickMath;

import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

/**
 * A single-producer, multiple consumer blocking concurrent queue of {@code long} values.
 */
public class SPMCLongArrayQueue {
    private static final AtomicLongFieldUpdater<SPMCLongArrayQueue> HEAD =
            AtomicLongFieldUpdater.newUpdater(SPMCLongArrayQueue.class, "head");

    private final AtomicLongArray queue;
    private final long nullSentinel;
    private final long mask;
    private final int capacity;

    private volatile long head;
    private volatile long tail;

    public SPMCLongArrayQueue(int requestedCapacity, long nullSentinel) {
        this.nullSentinel = nullSentinel;
        this.capacity = QuickMath.nextPowerOfTwo(requestedCapacity);
        this.mask = capacity - 1;
        this.queue = new AtomicLongArray(capacity);
    }

    public boolean offer(long value) {
        assert value != nullSentinel : "";
        final long acquiredTail = tail;
        if (acquiredTail - head >= capacity) {
            return false;
        }
        queue.lazySet(seqToArrayIndex(acquiredTail, mask), value);
        tail = acquiredTail + 1;
        return true;
    }

    public long poll() {
        while (true) {
            final long acquiredHead = head;
            if (acquiredHead >= tail) {
                return nullSentinel;
            }
            final int acquiredIndex = seqToArrayIndex(acquiredHead, mask);
            final long result = queue.get(acquiredIndex);
            if (HEAD.compareAndSet(this, acquiredHead, acquiredHead + 1)) {
                return result;
            }
        }
    }

    public long addedCount() {
        return tail;
    }

    public long removedCount() {
        return head;
    }

    static int seqToArrayIndex(long seq, long mask) {
        return (int) (seq & mask);
    }
}
