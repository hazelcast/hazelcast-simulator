package com.hazelcast.simulator.visualiser.data;

import com.hazelcast.simulator.probes.probes.Result;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class BenchmarkResults {

    private final Map<String, Result> probeDataMap  = new HashMap<String, Result>();
    private final String name;

    public BenchmarkResults(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public void addProbeData(String probeName, Result probeData) {
        probeDataMap.put(probeName, probeData);
    }

    public Set<String> getProbeNames() {
        return Collections.unmodifiableSet(probeDataMap.keySet());
    }

    public Result getProbeData(String probeName) {
        return probeDataMap.get(probeName);
    }
}
