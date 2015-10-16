package com.hazelcast.simulator.tests.special;

import com.hazelcast.simulator.probes.Probe;
import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.test.annotations.Run;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.SimulatorProbe;

import java.util.concurrent.TimeUnit;

import static com.hazelcast.simulator.utils.CommonUtils.sleepMillis;

/**
 * Used to verify the latency measurement with a superficial constant latency value.
 */
public class PerformanceMonitorLatencyTest {

    private static final long LATENCY_NANOS = TimeUnit.MICROSECONDS.toNanos(20);

    @SimulatorProbe(useForThroughput = true)
    Probe latencyProbe;

    private TestContext testContext;

    @Setup
    public void setup(TestContext testContext) {
        this.testContext = testContext;
    }

    @Run
    public void run() {
        while (!testContext.isStopped()) {
            latencyProbe.recordValue(LATENCY_NANOS);
            sleepMillis(1);
        }
    }
}
