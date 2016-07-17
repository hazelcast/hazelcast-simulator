/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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

import static javax.xml.bind.DatatypeConverter.parseBase64Binary;
import static org.HdrHistogram.Histogram.decodeFromCompressedByteBuffer;

/**
 * Responsible for storing and aggregating hdr histograms from Simulator workers.
 */
public class HdrHistogramContainer {

    private static final Logger LOGGER = Logger.getLogger(HdrHistogramContainer.class);

    private final ConcurrentMap<SimulatorAddress, HistogramsPerWorker> histogramsPerWorkerMap
            = new ConcurrentHashMap<SimulatorAddress, HistogramsPerWorker>();

    private final PerformanceStateContainer performanceStateContainer;

    public HdrHistogramContainer(PerformanceStateContainer performanceStateContainer) {
        this.performanceStateContainer = performanceStateContainer;
    }

    public synchronized void addHistograms(SimulatorAddress workerAddress, String testId, Map<String, String> histograms) {
        HistogramsPerWorker histogramsPerWorker = histogramsPerWorkerMap.get(workerAddress);
        if (histogramsPerWorker == null) {
            histogramsPerWorker = new HistogramsPerWorker();
            histogramsPerWorkerMap.put(workerAddress, histogramsPerWorker);
        }
        histogramsPerWorker.put(testId, histograms);
    }

    public ConcurrentMap<String, Map<String, String>> getHistograms(SimulatorAddress workerAddress) {
        HistogramsPerWorker histogramsPerWorker = histogramsPerWorkerMap.get(workerAddress);
        return histogramsPerWorker == null ? null : histogramsPerWorker.map;
    }

    void writeAggregatedHistograms(String testSuiteId, String testId) {
        PerformanceState performanceState = performanceStateContainer.get(testId);
        Result result = aggregateHistogramsForTestCase(testId, performanceState);
        if (result.isEmpty()) {
            return;
        }

        String fileName = "probes-" + testSuiteId + '_' + testId + ".xml";
        LOGGER.info("Writing histogram: " + fileName);
        ResultXmlUtils.toXml(result, new File(fileName));
    }

    private synchronized Result aggregateHistogramsForTestCase(String testId, PerformanceState state) {
        if (state == null) {
            return new ResultImpl(testId, 0, 0.0d);
        }

        Result result = new ResultImpl(testId, state.getOperationCount(), state.getTotalThroughput());
        for (HistogramsPerWorker histogramsPerWorker : histogramsPerWorkerMap.values()) {
            histogramsPerWorker.aggregate(testId, result);
        }
        return result;
    }

    private static class HistogramsPerWorker {
        // the key is the testId
        // the value is a map: where the key in this map is the probe-id and the value is the histogram
        private final ConcurrentMap<String, Map<String, String>> map
                = new ConcurrentHashMap<String, Map<String, String>>();

        private void put(String testId, Map<String, String> histograms) {
            map.put(testId, histograms);
        }

        private Map<String, String> get(String testId) {
            return map.get(testId);
        }

        private void aggregate(String testId, Result result) {
            Map<String, String> probeHistogramMap = get(testId);
            if (probeHistogramMap == null) {
                return;
            }

            for (Map.Entry<String, String> mapEntry : probeHistogramMap.entrySet()) {
                String probeName = mapEntry.getKey();
                String encodedHistogram = mapEntry.getValue();
                try {
                    ByteBuffer buffer = ByteBuffer.wrap(parseBase64Binary(encodedHistogram));
                    Histogram histogram = decodeFromCompressedByteBuffer(buffer, 0);
                    result.addHistogram(probeName, histogram);
                } catch (Exception e) {
                    LOGGER.warn("Could not decode histogram from test " + testId + " of probe " + probeName);
                }
            }
        }
    }
}
