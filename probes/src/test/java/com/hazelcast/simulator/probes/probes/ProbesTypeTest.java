package com.hazelcast.simulator.probes.probes;

import com.hazelcast.simulator.probes.probes.impl.DisabledProbe;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class ProbesTypeTest {

    @Test
    public void probesTypeNull() {
        assertNull(ProbesType.getProbeType("invalid"));
    }

    @Test
    public void probesTypeDISABLED() throws Exception {
        SimpleProbe probe = ProbesType.DISABLED.createInstance();
        assertTrue(probe == DisabledProbe.INSTANCE);
    }

    @Test
    public void probesTypeAssignableTHROUGHPUTtoSimpleProbe() {
        assertTrue(ProbesType.THROUGHPUT.isAssignableFrom(SimpleProbe.class));
    }

    @Test
    public void probesTypeAssignableTHROUGHPUTtoIntervalProbe() {
        assertTrue(ProbesType.THROUGHPUT.isAssignableFrom(IntervalProbe.class));
    }

    @Test
    public void probesTypeAssignableHDRtoSimpleProbe() {
        assertFalse(ProbesType.HDR.isAssignableFrom(SimpleProbe.class));
    }

    @Test
    public void probesTypeAssignableHDRtoIntervalProbe() {
        assertTrue(ProbesType.HDR.isAssignableFrom(IntervalProbe.class));
    }
}
