package com.hazelcast.stabilizer.common.probes;

import com.hazelcast.stabilizer.common.probes.impl.ConcurrentIntervalProbe;
import com.hazelcast.stabilizer.common.probes.impl.ConcurrentSimpleProbe;
import com.hazelcast.stabilizer.common.probes.impl.MaxLatencyProbe;
import com.hazelcast.stabilizer.common.probes.impl.OperationsPerSecProbe;

public class Probes {

    public static <R extends Result<R>, T extends SimpleProbe<R, T>> ConcurrentSimpleProbe<R, T> wrapAsThreadLocal(T probe) {
        return new ConcurrentSimpleProbe<R, T>(probe);
    }
    public static <R extends Result<R>, T extends IntervalProbe<R, T>> ConcurrentIntervalProbe<R, T> wrapAsThreadLocal(T probe) {
        return new ConcurrentIntervalProbe<R, T>(probe);
    }


    public static MaxLatencyProbe newMaxLatencyProbe() {
        return new MaxLatencyProbe();
    }

    public static OperationsPerSecProbe newOperationsPerSecProbe() {
        return new OperationsPerSecProbe();
    }
}
