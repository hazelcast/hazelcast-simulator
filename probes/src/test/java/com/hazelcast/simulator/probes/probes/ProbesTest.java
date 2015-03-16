package com.hazelcast.simulator.probes.probes;

import com.hazelcast.simulator.probes.probes.impl.ConcurrentProbe;
import com.hazelcast.simulator.probes.probes.impl.DisabledProbe;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class ProbesTest {

    private ProbesConfiguration config = new ProbesConfiguration();

    @Before
    public void setup() {
        config.addConfig("throughput", ProbesType.THROUGHPUT.string);
        config.addConfig("hdr", ProbesType.HDR.string);
        config.addConfig("invalid", "invalid");
    }

    @Test
    public void createDisabledProbe() {
        SimpleProbe probe1 = Probes.createProbe("disabled", IntervalProbe.class, config);
        SimpleProbe probe2 = Probes.createConcurrentProbe("disabled", SimpleProbe.class, config);

        assertTrue(probe1 instanceof DisabledProbe);
        assertTrue(probe2 instanceof DisabledProbe);
        assertTrue(probe1 == probe2);
    }

    @Test
    public void createSimpleProbe() {
        SimpleProbe probe = Probes.createProbe("throughput", SimpleProbe.class, config);
        assertNotNull(probe);
    }

    @Test
    public void createIntervalProbe() {
        IntervalProbe probe = Probes.createProbe("hdr", IntervalProbe.class, config);
        assertNotNull(probe);
    }

    @Test
    public void createConcurrentProbe() throws Exception {
        SimpleProbe probe = Probes.createConcurrentProbe("throughput", SimpleProbe.class, config);
        assertTrue(probe instanceof ConcurrentProbe);
    }

    @Test(expected = ClassCastException.class)
    public void createProbeTypeMismatch() {
        Probes.createConcurrentProbe("hdr", SimpleProbe.class, config);
    }

    @Test(expected = IllegalArgumentException.class)
    public void createInvalidProbeType() {
        Probes.createConcurrentProbe("invalid", SimpleProbe.class, config);
    }
}
