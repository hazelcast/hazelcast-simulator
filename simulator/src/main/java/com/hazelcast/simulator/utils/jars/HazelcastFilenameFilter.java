package com.hazelcast.simulator.utils.jars;

import java.io.File;
import java.io.FilenameFilter;

class HazelcastFilenameFilter implements FilenameFilter {

    @Override
    public boolean accept(File dir, String name) {
        if (!name.matches("^hazelcast-.*\\.jar$")) {
            return false;
        }
        if (name.matches(".*(sources|tests|code-generator).*")) {
            return false;
        }
        return true;
    }
}
