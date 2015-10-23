/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

@SuppressWarnings("checkstyle:finalclass")
public class CountdownWatch {

    private final long limit;

    private CountdownWatch(long delayMillis) {
        if (delayMillis < 0) {
            throw new IllegalArgumentException("Delay cannot be negative, passed " + delayMillis + '.');
        }

        long now = System.currentTimeMillis();
        long candidate = now + delayMillis;
        // overflow protection
        limit = (candidate >= now ? candidate : Long.MAX_VALUE);
    }

    public long getRemainingMs() {
        return Math.max(0, limit - System.currentTimeMillis());
    }

    public boolean isDone() {
        return System.currentTimeMillis() >= limit;
    }

    public static CountdownWatch started(long delayMillis) {
        return new CountdownWatch(delayMillis);
    }

    public static CountdownWatch unboundedStarted() {
        return new UnboundedCountdownWatch();
    }

    private static final class UnboundedCountdownWatch extends CountdownWatch {

        private UnboundedCountdownWatch() {
            super(Long.MAX_VALUE);
        }

        @Override
        public long getRemainingMs() {
            return Long.MAX_VALUE;
        }
    }
}
