package com.hazelcast.simulator.common;

public class CountdownWatch {
    private final long limit;

    private CountdownWatch(long delay) {
        if (delay < 0) {
            throw new IllegalArgumentException("Delay cannot be negative, passed " + delay + ".");
        }

        long now = System.currentTimeMillis();
        long candidate = now + delay;
        //overflow protection
        limit = candidate >= now ? candidate : Long.MAX_VALUE;
    }

    public long getRemainingMs() {
        return Math.max(0, limit - System.currentTimeMillis());
    }

    public boolean isDone() {
        return System.currentTimeMillis() >= limit;
    }

    public static CountdownWatch started(long delay) {
        return new CountdownWatch(delay);
    }

    public static CountdownWatch unboundedStarted() {
        return new UnboundedCountdownWatch();
    }

    private final static class UnboundedCountdownWatch extends CountdownWatch {
        private UnboundedCountdownWatch() {
            super(Long.MAX_VALUE);
        }

        @Override
        public long getRemainingMs() {
            return Long.MAX_VALUE;
        }
    }
}
