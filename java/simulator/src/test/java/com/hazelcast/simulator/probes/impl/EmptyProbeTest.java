package com.hazelcast.simulator.probes.impl;

import org.junit.Test;

import static org.junit.Assert.assertFalse;

public class EmptyProbeTest {

    @Test
    public void uselessTestToTriggerCoverageForThisClassWithoutFunctionality(){
        EmptyProbe emptyProbe = new EmptyProbe();
        emptyProbe.reset();
        emptyProbe.done(10);
        emptyProbe.recordValue(20);
        assertFalse(emptyProbe.isPartOfTotalThroughput());
    }
}
