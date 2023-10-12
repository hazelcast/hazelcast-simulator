package com.hazelcast.simulator.probes.impl;

import org.junit.Test;

import static org.junit.Assert.assertFalse;

public class EmptyLatencyProbeTest {

    @Test
    public void uselessTestToTriggerCoverageForThisClassWithoutFunctionality(){
        NoopLatencyProbe probe = new NoopLatencyProbe();
        probe.reset();
        probe.done(10);
        probe.recordValue(20);
        assertFalse(probe.includeInThroughput());
    }
}
