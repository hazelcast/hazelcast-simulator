package com.hazelcast.simulator.utils;

import com.hazelcast.logging.ILogger;

import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Delegating Logger. It throttles the rate messages are logged.
 */
public final class ThrottlingLogger {

    private final AtomicLong nextMessageNotBefore = new AtomicLong();

    private final ILogger delegate;
    private final long maximumRateNanos;

    private ThrottlingLogger(ILogger delegate, long maximumRateMs) {
        if (delegate == null) {
            throw new IllegalArgumentException("Logger cannot be null");
        }
        if (maximumRateMs <= 0) {
            throw new IllegalArgumentException("Maximum rate must be great than 0. Current rate: " + maximumRateMs);
        }

        this.delegate = delegate;
        this.maximumRateNanos = MILLISECONDS.toNanos(maximumRateMs);
    }

    public static ThrottlingLogger newLogger(ILogger delegate, long maximumRateMs) {
        return new ThrottlingLogger(delegate, maximumRateMs);
    }

    public void finest(String message) {
        log(Level.FINEST, message);
    }

    public void finer(String message) {
        log(Level.FINER, message);
    }

    public void fine(String message) {
        log(Level.FINE, message);
    }

    public void info(String message) {
        log(Level.INFO, message);
    }

    public void warn(String message) {
        log(Level.WARNING, message);
    }

    public void severe(String message) {
        log(Level.SEVERE, message);
    }

    public void log(Level level, String message) {
        if (!delegate.isLoggable(level)) {
            return;
        }

        long timeNow = System.nanoTime();
        long currentNotBefore = nextMessageNotBefore.get();

        if (timeNow < currentNotBefore) {
            // it's too soon to log
            return;
        }

        if (nextMessageNotBefore.compareAndSet(currentNotBefore, timeNow + maximumRateNanos)) {
            delegate.log(level, message);
        }
    }
}
