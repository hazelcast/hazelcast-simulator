package com.hazelcast.stabilizer.common.probes;

import com.hazelcast.stabilizer.common.probes.impl.ConcurrentIntervalProbe;
import com.hazelcast.stabilizer.common.probes.impl.ConcurrentSimpleProbe;
import com.hazelcast.stabilizer.common.probes.impl.LatencyDistributionProbe;
import com.hazelcast.stabilizer.common.probes.impl.MaxLatencyProbe;
import com.hazelcast.stabilizer.common.probes.impl.OperationsPerSecProbe;

public class Probes {

    private static <R extends Result<R>, T extends SimpleProbe<R, T>> ConcurrentSimpleProbe<R, T> wrapAsThreadLocal(T probe) {
        return new ConcurrentSimpleProbe<R, T>(probe);
    }
    private static <R extends Result<R>, T extends IntervalProbe<R, T>> ConcurrentIntervalProbe<R, T> wrapAsThreadLocal(T probe) {
        return new ConcurrentIntervalProbe<R, T>(probe);
    }

    public static <T extends SimpleProbe> T getDefaultProbe(Class<T> type) {
        if (type.equals(SimpleProbe.class)) {
            SimpleProbe probe = newOperationsPerSecProbe();
            return (T) probe;
        } else if (type.equals(IntervalProbe.class)) {
            IntervalProbe probe = newLatencyDistributionProbe();
            return (T) probe;
        } else {
            throw new IllegalArgumentException("Unknown probe type "+type.getName());
        }
    }

    public static <T extends IntervalProbe> IntervalProbe newMaxLatencyProbe() {
        return Probes.wrapAsThreadLocal(new MaxLatencyProbe());
    }

    public static SimpleProbe newOperationsPerSecProbe() {
        return Probes.wrapAsThreadLocal(new OperationsPerSecProbe());
    }

    public static IntervalProbe newLatencyDistributionProbe() {
        return Probes.wrapAsThreadLocal(new LatencyDistributionProbe());
    }
}
