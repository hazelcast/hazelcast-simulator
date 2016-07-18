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

import static com.hazelcast.simulator.probes.impl.HdrProbe.LATENCY_PRECISION;
import static com.hazelcast.simulator.probes.impl.HdrProbe.MAXIMUM_LATENCY;
import static com.hazelcast.simulator.utils.FileUtils.deleteQuiet;
import static com.hazelcast.simulator.utils.TestUtils.createTmpDirectory;
import static java.util.Collections.singletonMap;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class HdrHistogramContainerTest {

    private File hdrFile;
    private File outputDirectory;
    private SimulatorAddress workerAddress1 = new SimulatorAddress(AddressLevel.WORKER, 1, 1, 0);
    private SimulatorAddress workerAddress2 = new SimulatorAddress(AddressLevel.WORKER, 1, 2, 0);
    private HdrHistogramContainer hdrHistogramContainer;

    @Before
    public void setUp() {
        PerformanceState performanceState = new PerformanceState();

        PerformanceStateContainer performanceStateContainer = mock(PerformanceStateContainer.class);
        when(performanceStateContainer.get("testId")).thenReturn(performanceState);

        outputDirectory = createTmpDirectory();
        hdrFile = new File(outputDirectory, "testSuiteId_testId_workerProbe.hdr");
        hdrHistogramContainer = new HdrHistogramContainer(outputDirectory, performanceStateContainer);
    }

    @After
    public void tearDown() {
        deleteQuiet(outputDirectory);
    }

    @Test
    public void testCreateProbeResults() {
        String histogram1 = createEncodedHistogram();
        hdrHistogramContainer.addHistograms(workerAddress1, "testId", singletonMap("workerProbe", histogram1));

        String histogram2 = createEncodedHistogram();
        hdrHistogramContainer.addHistograms(workerAddress2, "testId", singletonMap("workerProbe", histogram2));

        hdrHistogramContainer.writeAggregatedHistograms("testSuiteId", "testId");
        assertTrue(hdrFile.getAbsolutePath() + " does not exist", hdrFile.exists());
    }

    @Test
    public void testCreateProbeResults_noHistogramForTestId() {
        String histogram = createEncodedHistogram();
        hdrHistogramContainer.addHistograms(workerAddress1, "anotherTestId", singletonMap("workerProbe", histogram));

        hdrHistogramContainer.writeAggregatedHistograms("testSuiteId", "testId");
        assertFalse(hdrFile.exists());
    }

    @Test
    public void testCreateProbeResults_invalidHistogram() {
        hdrHistogramContainer.addHistograms(workerAddress1, "testId", singletonMap("workerProbe", "invalidHistogram"));

        hdrHistogramContainer.writeAggregatedHistograms("testSuiteId", "testId");
        assertFalse(hdrFile.exists());
    }

    @Test
    public void testCreateProbeResults_unknownTestId() {
        hdrHistogramContainer.writeAggregatedHistograms("testSuiteId", "unknownTestId");
        assertFalse(hdrFile.exists());
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
