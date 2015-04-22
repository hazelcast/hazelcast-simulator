package com.hazelcast.simulator.probes.probes;

import com.hazelcast.simulator.probes.probes.impl.DisabledProbe;
import com.hazelcast.simulator.probes.probes.impl.HdrLatencyDistributionProbe;
import com.hazelcast.simulator.probes.probes.impl.LatencyDistributionProbe;
import com.hazelcast.simulator.probes.probes.impl.MaxLatencyProbe;
import com.hazelcast.simulator.probes.probes.impl.OperationsPerSecProbe;
import com.hazelcast.simulator.probes.probes.impl.WorkerProbe;

/**
 * Defines all probe types and the interface type which they can be assigned to.
 */
public enum ProbesType {

    DISABLED("disabled", SimpleProbe.class, DisabledProbe.class),
    WORKER("worker", SimpleProbe.class, WorkerProbe.class),
    THROUGHPUT("throughput", SimpleProbe.class, OperationsPerSecProbe.class),
    LATENCY("latency", IntervalProbe.class, LatencyDistributionProbe.class),
    MAX_LATENCY("maxLatency", IntervalProbe.class, MaxLatencyProbe.class),
    HDR("hdr", IntervalProbe.class, HdrLatencyDistributionProbe.class);

    private final String string;

    private final Class<? extends SimpleProbe> assignableClassType;
    private final Class<? extends SimpleProbe> probeClassType;

    ProbesType(String string, Class<? extends SimpleProbe> assignableClassType, Class<? extends SimpleProbe> probeClassType) {
        this.string = string;
        this.assignableClassType = assignableClassType;
        this.probeClassType = probeClassType;
    }

    public static ProbesType getProbeType(String probeType) {
        for (ProbesType probesType : ProbesType.values()) {
            if (probesType.matches(probeType)) {
                return probesType;
            }
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    public <T extends IntervalProbe> T createInstance() throws Exception {
        if (this == DISABLED) {
            return (T) DisabledProbe.INSTANCE;
        }
        return (T) probeClassType.newInstance();
    }

    public boolean isAssignableFrom(Class<? extends SimpleProbe> classType) {
        return this.assignableClassType.isAssignableFrom(classType);
    }

    public boolean matches(String probeName) {
        return string.equals(probeName);
    }

    public String getName() {
        return string;
    }
}
