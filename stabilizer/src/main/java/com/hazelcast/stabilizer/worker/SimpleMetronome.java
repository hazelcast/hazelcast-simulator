package com.hazelcast.stabilizer.worker;

public class SimpleMetronome implements Metronome {
    private final int intervalMs;
    private long nextNotBefore;

    private SimpleMetronome(int intervalMs) {
        this.nextNotBefore = 0;
        this.intervalMs = intervalMs;
    }

    public static Metronome withFixedIntervalMs(int intervalMs) {
        if (intervalMs == 0) {
            return new EmptyMetronome();
        }
        return new SimpleMetronome(intervalMs);
    }

    @Override
    public void waitForNext() {
        long timestamp;
        while ((timestamp = System.currentTimeMillis()) < nextNotBefore) {
            //noop
        }
        nextNotBefore = timestamp + intervalMs;
    }

    private static class EmptyMetronome implements Metronome {
        @Override
        public void waitForNext() {
            //noop
        }
    }
}
