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
package com.hazelcast.simulator.utils;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.util.concurrent.atomic.AtomicLong;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Delegating Logger. It throttles the rate messages are logged.
 */
public final class ThrottlingLogger {

    private final AtomicLong nextMessageNotBefore = new AtomicLong();

    private final Logger delegate;
    private final long maximumRateNanos;

    public ThrottlingLogger(Logger delegate, long maximumRateMs) {
        if (delegate == null) {
            throw new IllegalArgumentException("Logger cannot be null");
        }

        if (maximumRateMs <= 0) {
            throw new IllegalArgumentException("Maximum rate must be great than 0. Current rate: " + maximumRateMs);
        }

        this.delegate = delegate;
        this.maximumRateNanos = MILLISECONDS.toNanos(maximumRateMs);
    }

    public static ThrottlingLogger newLogger(Logger delegate, long maximumRateMs) {
        return new ThrottlingLogger(delegate, maximumRateMs);
    }

    public void finest(String message) {
        log(Level.TRACE, message);
    }

    public void finer(String message) {
        log(Level.TRACE, message);
    }

    public void fine(String message) {
        log(Level.DEBUG, message);
    }

    public void info(String message) {
        log(Level.INFO, message);
    }

    public void warn(String message) {
        log(Level.WARN, message);
    }

    public void severe(String message) {
        log(Level.FATAL, message);
    }

    public void log(Level level, String message) {
        if (!delegate.isEnabledFor(level)) {
            return;
        }

        if (!requestLogSlot()) {
            return;
        }

        delegate.log(level, message);
    }

    public boolean requestLogSlot() {
        long timeNow = System.nanoTime();
        long currentNotBefore = nextMessageNotBefore.get();

        if (timeNow < currentNotBefore) {
            // it's too soon to log
            return false;
        }

        return nextMessageNotBefore.compareAndSet(currentNotBefore, timeNow + maximumRateNanos);
    }

    public void logInSlot(Level level, String message) {
        if (!delegate.isEnabledFor(level)) {
            return;
        }

        delegate.log(level, message);
    }
}
