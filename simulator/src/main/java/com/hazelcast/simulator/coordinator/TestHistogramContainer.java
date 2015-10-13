package com.hazelcast.simulator.coordinator;

import com.hazelcast.simulator.probes.Result;
import com.hazelcast.simulator.probes.impl.ResultImpl;
import com.hazelcast.simulator.probes.xml.ResultXmlUtils;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.worker.performance.PerformanceState;
import org.HdrHistogram.Histogram;
import org.apache.log4j.Logger;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static java.lang.String.format;
import static javax.xml.bind.DatatypeConverter.parseBase64Binary;
import static org.HdrHistogram.Histogram.decodeFromCompressedByteBuffer;

/**
 * Responsible for storing and aggregating test histograms from Simulator workers.
 */
public class TestHistogramContainer {

    private static final Logger LOGGER = Logger.getLogger(TestHistogramContainer.class);

    private final ConcurrentMap<SimulatorAddress, ConcurrentMap<String, Map<String, String>>> workerTestProbeHistogramMap
            = new ConcurrentHashMap<SimulatorAddress, ConcurrentMap<String, Map<String, String>>>();

    private final PerformanceStateContainer performanceStateContainer;

    public TestHistogramContainer(PerformanceStateContainer performanceStateContainer) {
        this.performanceStateContainer = performanceStateContainer;
    }

    public synchronized void addTestHistograms(SimulatorAddress workerAddress, String testId, Map<String, String> histograms) {
        ConcurrentMap<String, Map<String, String>> testHistogramMap = workerTestProbeHistogramMap.get(workerAddress);
        if (testHistogramMap == null) {
            testHistogramMap = new ConcurrentHashMap<String, Map<String, String>>();
            workerTestProbeHistogramMap.put(workerAddress, testHistogramMap);
        }
        testHistogramMap.put(testId, histograms);
    }

    void createProbeResults(String testSuiteId, String testCaseId) {
        PerformanceState performanceState = performanceStateContainer.getPerformanceStateForTestCase(testCaseId);
        Result result = aggregateHistogramsForTestCase(testCaseId, performanceState);
        if (!result.isEmpty()) {
            String fileName = "probes-" + testSuiteId + "_" + testCaseId + ".xml";
            ResultXmlUtils.toXml(result, new File(fileName));
            logProbesResultInHumanReadableFormat(testCaseId, result);
        }
    }

    synchronized Result aggregateHistogramsForTestCase(String testCaseId, PerformanceState state) {
        Result result = new ResultImpl(testCaseId, state.getOperationCount(), state.getTotalThroughput());
        for (ConcurrentMap<String, Map<String, String>> testHistogramMap : workerTestProbeHistogramMap.values()) {
            Map<String, String> probeHistogramMap = testHistogramMap.get(testCaseId);
            for (Map.Entry<String, String> mapEntry : probeHistogramMap.entrySet()) {
                String probeName = mapEntry.getKey();
                String encodedHistogram = mapEntry.getValue();
                try {
                    ByteBuffer buffer = ByteBuffer.wrap(parseBase64Binary(encodedHistogram));
                    Histogram histogram = decodeFromCompressedByteBuffer(buffer, 0);
                    result.addHistogram(probeName, histogram);
                } catch (Exception e) {
                    LOGGER.warn("Could not decode histogram from test " + testCaseId + " of probe " + probeName);
                }
            }
        }
        return result;
    }

    private void logProbesResultInHumanReadableFormat(String testId, Result result) {
        for (String probeName : result.probeNames()) {
            LOGGER.info(format("%s Results of probe %s:%n%s", testId, probeName, result.toHumanString(probeName)));
        }
    }
}
