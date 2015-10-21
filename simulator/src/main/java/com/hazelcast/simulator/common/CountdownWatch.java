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
