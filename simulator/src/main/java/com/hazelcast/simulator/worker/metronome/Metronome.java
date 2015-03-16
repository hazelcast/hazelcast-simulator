package com.hazelcast.simulator.worker.metronome;

/**
 * Used to clock a running task or worker with a defined interval.
 */
public interface Metronome {

    /**
     * Waits for the defined interval.
     */
    void waitForNext();
}
