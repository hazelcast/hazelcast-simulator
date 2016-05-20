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
package com.hazelcast.simulator.probes.impl;

import com.hazelcast.simulator.probes.Result;
import org.HdrHistogram.Histogram;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static com.hazelcast.simulator.utils.CommonUtils.closeQuietly;

public class ResultImpl implements Result {

    private final String testName;
    private final long invocations;
    private final double throughput;

    private final Map<String, Histogram> probeHistogramMap;

    public ResultImpl(String testName, long invocations, double throughput) {
        this.testName = testName;
        this.invocations = invocations;
        this.throughput = throughput;

        this.probeHistogramMap = new HashMap<String, Histogram>();
    }

    @Override
    public String getTestName() {
        return testName;
    }

    @Override
    public long getInvocations() {
        return invocations;
    }

    @Override
    public double getThroughput() {
        return throughput;
    }

    @Override
    public boolean isEmpty() {
        return probeHistogramMap.isEmpty();
    }

    @Override
    public void addHistogram(String probeName, Histogram histogram) {
        if (histogram == null) {
            return;
        }

        Histogram candidate = probeHistogramMap.get(probeName);
        if (candidate == null) {
            probeHistogramMap.put(probeName, histogram);
            return;
        }

        candidate.add(histogram);
    }

    @Override
    public Histogram getHistogram(String probeName) {
        return probeHistogramMap.get(probeName);
    }

    @Override
    public Set<String> probeNames() {
        return probeHistogramMap.keySet();
    }

    @Override
    public String toHumanString(String probeName) {
        Histogram histogram = probeHistogramMap.get(probeName);
        if (histogram == null) {
            return null;
        }

        ByteArrayOutputStream outputStream = null;
        PrintStream stream = null;
        try {
            outputStream = new ByteArrayOutputStream();
            stream = new PrintStream(outputStream, true, "UTF-8");

            histogram.outputPercentileDistribution(stream, 1.0);
            return new String(outputStream.toByteArray(), "UTF-8");
        } catch (Exception e) {
            return null;
        } finally {
            closeQuietly(stream);
            closeQuietly(outputStream);
        }
    }
}
