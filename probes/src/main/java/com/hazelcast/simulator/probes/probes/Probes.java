package com.hazelcast.simulator.probes.probes;

import com.hazelcast.simulator.probes.probes.impl.ConcurrentIntervalProbe;
import com.hazelcast.simulator.probes.probes.impl.ConcurrentSimpleProbe;
import com.hazelcast.simulator.probes.probes.impl.DisabledProbe;
import com.hazelcast.simulator.probes.probes.impl.HdrLatencyDistributionProbe;
import com.hazelcast.simulator.probes.probes.impl.LatencyDistributionProbe;
import com.hazelcast.simulator.probes.probes.impl.MaxLatencyProbe;
import com.hazelcast.simulator.probes.probes.impl.OperationsPerSecProbe;

import static java.lang.String.format;

public final class Probes {

    private Probes() {

    }

    @SuppressWarnings("unchecked")
    public static <T extends SimpleProbe> T createProbe(Class<T> type, String name, ProbesConfiguration probesConfiguration) {
        String config = probesConfiguration.getConfig(name);
        if (SimpleProbe.class.equals(type)) {
            if (config == null) {
                return (T) newDefaultSimpleProbe();
            } else if ("throughput".equals(config)) {
                return (T) newOperationsPerSecProbe();
            } else if ("disabled".equals(config)) {
                return (T) disabledProbe();
            }
        } else if (IntervalProbe.class.equals(type)) {
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
            }
        }
        throw new IllegalArgumentException(format(
                "Unknown probe %s for probe type %s.", config, (type != null) ? type.getName() : null));
    }

    private static SimpleProbe newDefaultSimpleProbe() {
        return disabledProbe();
    }

    private static SimpleProbe newOperationsPerSecProbe() {
        return wrapAsThreadLocal(new OperationsPerSecProbe());
    }

    private static IntervalProbe newDefaultIntervalProbe() {
        return disabledProbe();
    }

    private static IntervalProbe newLatencyDistributionProbe() {
        return wrapAsThreadLocal(new LatencyDistributionProbe());
    }

    private static IntervalProbe newMaxLatencyProbe() {
        return wrapAsThreadLocal(new MaxLatencyProbe());
    }

    private static IntervalProbe hdrProbe() {
        return wrapAsThreadLocal(new HdrLatencyDistributionProbe());
    }

    private static IntervalProbe disabledProbe() {
        return DisabledProbe.INSTANCE;
    }

    private static <R extends Result<R>, T extends SimpleProbe<R, T>> ConcurrentSimpleProbe<R, T> wrapAsThreadLocal(T probe) {
        return new ConcurrentSimpleProbe<R, T>(probe);
    }

    private static <R extends Result<R>, T extends IntervalProbe<R, T>> ConcurrentIntervalProbe<R, T> wrapAsThreadLocal(T probe) {
        return new ConcurrentIntervalProbe<R, T>(probe);
    }
}
