package com.hazelcast.simulator.coordinator;

import com.hazelcast.simulator.protocol.core.AddressLevel;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.worker.performance.PerformanceState;
import org.HdrHistogram.Histogram;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.xml.bind.DatatypeConverter;
import java.io.File;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Random;
import java.util.zip.Deflater;

import static com.hazelcast.simulator.probes.impl.ProbeImpl.LATENCY_PRECISION;
import static com.hazelcast.simulator.probes.impl.ProbeImpl.MAXIMUM_LATENCY;
import static com.hazelcast.simulator.utils.FileUtils.deleteQuiet;
import static java.util.Collections.singletonMap;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestHistogramContainerTest {

    private File probeFile = new File("probes-testSuiteId_testId.xml");
    private SimulatorAddress workerAddress1 = new SimulatorAddress(AddressLevel.WORKER, 1, 1, 0);
    private SimulatorAddress workerAddress2 = new SimulatorAddress(AddressLevel.WORKER, 1, 2, 0);

    private TestHistogramContainer testHistogramContainer;

    @Before
    public void setUp() {
        PerformanceState performanceState = new PerformanceState();

        PerformanceStateContainer performanceStateContainer = mock(PerformanceStateContainer.class);
        when(performanceStateContainer.getPerformanceStateForTestCase("testId")).thenReturn(performanceState);

        testHistogramContainer = new TestHistogramContainer(performanceStateContainer);
    }

    @After
    public void tearDown() {
        deleteQuiet(probeFile);
    }

    @Test
    public void testCreateProbeResults() {
        String histogram1 = createEncodedHistogram();
        testHistogramContainer.addTestHistograms(workerAddress1, "testId", singletonMap("workerProbe", histogram1));

        String histogram2 = createEncodedHistogram();
        testHistogramContainer.addTestHistograms(workerAddress2, "testId", singletonMap("workerProbe", histogram2));

        testHistogramContainer.createProbeResults("testSuiteId", "testId");
        assertTrue(probeFile.exists());
    }

    @Test
    public void testCreateProbeResults_noHistogramForTestId() {
        String histogram = createEncodedHistogram();
        testHistogramContainer.addTestHistograms(workerAddress1, "anotherTestId", singletonMap("workerProbe", histogram));

        testHistogramContainer.createProbeResults("testSuiteId", "testId");
        assertFalse(probeFile.exists());
    }

    @Test
    public void testCreateProbeResults_invalidHistogram() {
        testHistogramContainer.addTestHistograms(workerAddress1, "testId", singletonMap("workerProbe", "invalidHistogram"));

        testHistogramContainer.createProbeResults("testSuiteId", "testId");
        assertFalse(probeFile.exists());
    }

    @Test
    public void testCreateProbeResults_unknownTestId() {
        testHistogramContainer.createProbeResults("testSuiteId", "unknownTestId");
        assertFalse(probeFile.exists());
    }

    private static String createEncodedHistogram() {
        Random random = new Random();

        Histogram histogram = new Histogram(MAXIMUM_LATENCY, LATENCY_PRECISION);
        for (int i = 0; i < 10; i++) {
            histogram.recordValue(random.nextInt((int) MAXIMUM_LATENCY));
        }
        return getEncodedHistogram(histogram);
    }

    private static String getEncodedHistogram(Histogram combined) {
        ByteBuffer targetBuffer = ByteBuffer.allocate(combined.getNeededByteBufferCapacity());
        int compressedLength = combined.encodeIntoCompressedByteBuffer(targetBuffer, Deflater.BEST_COMPRESSION);
        byte[] compressedArray = Arrays.copyOf(targetBuffer.array(), compressedLength);
        return DatatypeConverter.printBase64Binary(compressedArray);
    }
}
