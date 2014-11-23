package com.hazelcast.stabilizer.probes.probes;

import com.hazelcast.stabilizer.probes.probes.impl.HdrLatencyDistributionProbe;
import com.hazelcast.stabilizer.probes.probes.impl.MaxLatencyProbe;
import com.hazelcast.stabilizer.probes.probes.impl.ConcurrentIntervalProbe;
import com.hazelcast.stabilizer.probes.probes.impl.ConcurrentSimpleProbe;
import com.hazelcast.stabilizer.probes.probes.impl.DisabledProbe;
import com.hazelcast.stabilizer.probes.probes.impl.LatencyDistributionProbe;
import com.hazelcast.stabilizer.probes.probes.impl.OperationsPerSecProbe;

public class Probes {

    private static <R extends Result<R>, T extends SimpleProbe<R, T>> ConcurrentSimpleProbe<R, T> wrapAsThreadLocal(T probe) {
        return new ConcurrentSimpleProbe<R, T>(probe);
    }

    private static <R extends Result<R>, T extends IntervalProbe<R, T>> ConcurrentIntervalProbe<R, T> wrapAsThreadLocal(T probe) {
        return new ConcurrentIntervalProbe<R, T>(probe);
    }

    public static <T extends SimpleProbe> T createProbe(Class<T> type, String name, ProbesConfiguration probesConfiguration) {
        String config = probesConfiguration.getConfig(name);
        if (type.equals(SimpleProbe.class)) {
            if (config == null) {
                return (T) newDefaultSimpleProbe();
            } else if ("throughput".equals(config)) {
                return (T) newOperationsPerSecProbe();
            } else if ("disabled".equals(config)) {
                return (T) disabledProbe();
            } else {
                throw new IllegalArgumentException("Unknown probe " + config + " for probe type " + type.getName() + ".");
            }
        } else if (type.equals(IntervalProbe.class)) {
            if (config == null) {
                return (T) newDefaultIntervalProbe();
            } else if ("latency".equals(config)) {
                return (T) newLatencyDistributionProbe();
            } else if ("maxLatency".equals(config)) {
                return (T) newMaxLatencyProbe();
            } else if ("disabled".equals(config)) {
                return (T) disabledProbe();
            } else if ("hdr".equals(config)) {
                return (T) hdrProbe();
            } else {
                throw new IllegalArgumentException("Unknown probe " + config + " for probe type " + type.getName() + ".");
            }
        } else {
            throw new IllegalArgumentException("Unknown probe " + config + " for probe type " + type.getName() + ".");
        }
    }

    private static IntervalProbe hdrProbe() {
        return Probes.wrapAsThreadLocal(new HdrLatencyDistributionProbe());
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

    public static IntervalProbe disabledProbe() {
        return DisabledProbe.INSTANCE;
    }

    private static SimpleProbe newDefaultSimpleProbe() {
        return disabledProbe();
    }

    private static IntervalProbe newDefaultIntervalProbe() {
        return disabledProbe();
    }

}
