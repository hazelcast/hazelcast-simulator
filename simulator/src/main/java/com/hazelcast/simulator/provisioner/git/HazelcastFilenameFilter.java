package com.hazelcast.simulator.provisioner.git;

import java.io.File;
import java.io.FilenameFilter;

public class HazelcastFilenameFilter implements FilenameFilter {

    @Override
    public boolean accept(File dir, String name) {
        if (!name.endsWith(".jar")) {
            return false;
        }
        if (!name.contains("hazelcast")) {
            return false;
        }
        if (name.contains("sources")) {
            return false;
        }
        if (name.contains("tests")) {
            return false;
        }
        if (name.contains("original")) {
            return false;
        }
        return true;
    }
}
