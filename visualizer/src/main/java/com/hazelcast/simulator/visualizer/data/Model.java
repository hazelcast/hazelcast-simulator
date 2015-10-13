/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast.simulator.visualizer.data;

import com.hazelcast.simulator.probes.Result;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EventListener;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class Model {

    private final Map<String, Result> benchmarks = new HashMap<String, Result>();
    private final List<BenchmarkChangeListener> listeners = new ArrayList<BenchmarkChangeListener>();

    public void addResults(Result benchmarkResults) {
        String name = benchmarkResults.getTestName();
        benchmarks.put(name, benchmarkResults);
        changed(name);
    }

    public Set<String> getBenchmarkNames() {
        return Collections.unmodifiableSet(benchmarks.keySet());
    }

    public Result getBenchmarkResult(String name) {
        return benchmarks.get(name);
    }

    public void addBenchmarkChangeListener(BenchmarkChangeListener listener) {
        listeners.add(listener);
    }

    private void changed(String name) {
        for (BenchmarkChangeListener listener : listeners) {
            listener.benchmarkChanged(name);
        }
    }

    public interface BenchmarkChangeListener extends EventListener {
        void benchmarkChanged(String benchmarkName);
    }
}
