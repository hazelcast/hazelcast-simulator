package com.hazelcast.simulator.coordinator;

import com.hazelcast.simulator.probes.probes.ProbesResultXmlWriter;
import com.hazelcast.simulator.probes.probes.Result;
import com.hazelcast.simulator.probes.probes.impl.ResultImpl;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.worker.performance.PerformanceState;
import org.HdrHistogram.Histogram;
import org.apache.log4j.Logger;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.zip.DataFormatException;

import static java.lang.String.format;
import static javax.xml.bind.DatatypeConverter.parseBase64Binary;
import static org.HdrHistogram.Histogram.decodeFromCompressedByteBuffer;

/**
 * Responsible for storing and aggregating test histograms from Simulator workers.
 */
public class TestHistogramContainer {

    private static final Logger LOGGER = Logger.getLogger(TestHistogramContainer.class);

    private final ConcurrentMap<SimulatorAddress, ConcurrentMap<String, Map<String, String>>> workerTestHistogramMap
            = new ConcurrentHashMap<SimulatorAddress, ConcurrentMap<String, Map<String, String>>>();

    private final PerformanceStateContainer performanceStateContainer;

    public TestHistogramContainer(PerformanceStateContainer performanceStateContainer) {
        this.performanceStateContainer = performanceStateContainer;
    }

    public synchronized void addTestHistograms(SimulatorAddress workerAddress, String testId, Map<String, String> histograms) {
        ConcurrentMap<String, Map<String, String>> testHistogramMap = workerTestHistogramMap.get(workerAddress);
        if (testHistogramMap == null) {
            testHistogramMap = new ConcurrentHashMap<String, Map<String, String>>();
            workerTestHistogramMap.put(workerAddress, testHistogramMap);
        }
        testHistogramMap.put(testId, histograms);
    }

    void createProbeResults(String testSuiteId, String testCaseId) {
        PerformanceState performanceState = performanceStateContainer.getPerformanceStateForTestCase(testCaseId);
        Map<String, Result> probesResult = aggregateHistogramsForTestCase(testCaseId, performanceState);
        if (!probesResult.isEmpty()) {
            String fileName = "probes-" + testSuiteId + "_" + testCaseId + ".xml";
            ProbesResultXmlWriter.write(probesResult, new File(fileName));
            logProbesResultInHumanReadableFormat(testCaseId, probesResult);
        }
    }

    synchronized Map<String, Result> aggregateHistogramsForTestCase(String testCaseId, PerformanceState state) {
        Map<String, Result> probeResults = new HashMap<String, Result>();
        for (ConcurrentMap<String, Map<String, String>> testHistogramMap : workerTestHistogramMap.values()) {
            Map<String, String> probeHistogramMap = testHistogramMap.get(testCaseId);
            for (Map.Entry<String, String> mapEntry : probeHistogramMap.entrySet()) {
                String probeName = mapEntry.getKey();
                String encodedHistogram = mapEntry.getValue();
                try {
                    ByteBuffer buffer = ByteBuffer.wrap(parseBase64Binary(encodedHistogram));
                    Histogram histogram = decodeFromCompressedByteBuffer(buffer, 0);
                    Result result = probeResults.get(probeName);
                    if (result != null) {
                        result.getHistogram().add(histogram);
                    } else {
                        result = new ResultImpl(histogram, state.getOperationCount(), state.getTotalThroughput());
                        probeResults.put(probeName, result);
                    }
                } catch (DataFormatException e) {
                    LOGGER.warn("Could not parse encoded histogram from test " + testCaseId + " of probe " + probeName);
                }
            }
        }
        return probeResults;
    }

    private void logProbesResultInHumanReadableFormat(String testId, Map<String, Result> combinedResults) {
        for (Map.Entry<String, Result> entry : combinedResults.entrySet()) {
            String probeName = entry.getKey();
            Result result = entry.getValue();
            LOGGER.info(format("Results for test %s of probe %s:%n%s", testId, probeName, result.toHumanString()));
        }
    }
}
