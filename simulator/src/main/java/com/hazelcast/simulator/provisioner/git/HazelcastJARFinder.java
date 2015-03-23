package com.hazelcast.simulator.provisioner.git;

import org.apache.log4j.Logger;

import java.io.File;

import static com.hazelcast.simulator.utils.CommonUtils.exitWithError;
import static com.hazelcast.simulator.utils.FileUtils.newFile;

public class HazelcastJARFinder {

    private static final Logger LOGGER = Logger.getLogger(HazelcastJARFinder.class);

    public File[] find(File path) {
        File memberPath = newFile(path, "hazelcast", "target");
        File memberJar = findJarAtPath(memberPath);
        File clientPath = newFile(path, "hazelcast-client", "target");
        File clientJar = findJarAtPath(clientPath);
        return new File[]{memberJar, clientJar};
    }

    private File findJarAtPath(File memberPath) {
        checkPathExist(memberPath);
        return findHazelcastJar(memberPath);
    }

    private File findHazelcastJar(File memberPathToTarget) {
        File[] files = memberPathToTarget.listFiles(new HazelcastFilenameFilter());
        checkIsSingleFile(files, memberPathToTarget);
        return files[0];
    }

    private void checkIsSingleFile(File[] files, File memberPathToTarget) {
        if (files.length == 0) {
            exitWithError(LOGGER, "Path " + memberPathToTarget + " doesn't contain Hazelcast JAR.");
        }
        if (files.length > 1) {
            StringBuilder sb = new StringBuilder("Path ")
                    .append(memberPathToTarget.getAbsolutePath())
                    .append("contains more than one JAR which seems to be Hazelcast JAR: \n");
            for (File file : files) {
                sb.append(file.getName()).append('\n');
            }
            sb.append("This is probably a bug, please create a new bug report with this message. "
                    + "https://github.com/hazelcast/hazelcast-simulator/issues/new");
            exitWithError(LOGGER, sb.toString());
        }
    }

    private void checkPathExist(File memberPathToTarget) {
        if (!memberPathToTarget.exists()) {
            exitWithError(LOGGER, "Cannot find a path to Hazelcast JAR. It should be at " + memberPathToTarget.getAbsolutePath()
                    + ", but the path doesn't exist.");
        }
    }
}
