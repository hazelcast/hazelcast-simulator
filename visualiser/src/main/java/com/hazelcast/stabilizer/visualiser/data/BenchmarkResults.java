package com.hazelcast.stabilizer.visualiser.data;


import com.hazelcast.stabilizer.probes.probes.Result;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class BenchmarkResults {
    private String name;
    private final Map<String, Result> probeDataMap;

    public BenchmarkResults(String name) {
        this.name = name;
        this.probeDataMap = new HashMap<String, Result>();
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
