package com.hazelcast.simulator;

import org.apache.log4j.Logger;

import java.io.File;

import static com.hazelcast.simulator.utils.FileUtils.deleteQuiet;
import static com.hazelcast.simulator.utils.FileUtils.getSimulatorHome;

public class TestEnvironmentUtils {

    private static final Logger LOGGER = Logger.getLogger(TestEnvironmentUtils.class);

    private static String originalUserDir;

    public static void setDistributionUserDir() {
        originalUserDir = System.getProperty("user.dir");
        System.setProperty("user.dir", originalUserDir + "/dist/src/main/dist");

        LOGGER.info("original userDir: " + originalUserDir);
        LOGGER.info("actual userDir: " + System.getProperty("user.dir"));
        LOGGER.info("SIMULATOR_HOME: " + getSimulatorHome());
    }

    public static void resetUserDir() {
        System.setProperty("user.dir", originalUserDir);
    }

    public static void deleteLogs() {
        deleteQuiet(new File("dist/src/main/dist/workers"));
        deleteQuiet(new File("logs"));
        deleteQuiet(new File("workers"));
    }
}
