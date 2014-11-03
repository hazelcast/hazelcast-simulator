package com.hazelcast.stabilizer.probes.probes;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class ProbesConfiguration implements Serializable {
    private final Map<String, String> config;

    public ProbesConfiguration() {
        this.config = new HashMap<String, String>();
    }

    public void addConfig(String name, String configuration) {
        config.put(name, configuration);
    }

    public String getConfig(String name) {
        return config.get(name);
    }
}
