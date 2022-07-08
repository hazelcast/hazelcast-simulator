package com.hazelcast.simulator;

import com.hazelcast.simulator.utils.FileUtils;
import com.hazelcast.simulator.utils.TestUtils;
import com.hazelcast.simulator.utils.UncheckedIOException;
import com.hazelcast.simulator.utils.helper.ExitExceptionSecurityManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;

import static com.hazelcast.simulator.utils.FileUtils.appendText;
import static com.hazelcast.simulator.utils.FileUtils.getUserDir;

public class TestEnvironmentUtils {

    private static final Logger LOGGER = LogManager.getLogger(TestEnvironmentUtils.class);

    private static SecurityManager originalSecurityManager;

    private static String originalSimulatorHome;

    private static File inventoryFile;

    public static File setupFakeEnvironment() {
        File dist = internalDistDirectory();
        File simulatorHome = TestUtils.createTmpDirectory();

        try {
            copyDirectory(dist, simulatorHome);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        System.setProperty("user.dir.test", simulatorHome.getAbsolutePath());

        LOGGER.info("Fake SIMULATOR_HOME:" + simulatorHome.getAbsolutePath());

        originalSimulatorHome = System.getProperty("SIMULATOR_HOME");
        System.setProperty("SIMULATOR_HOME", simulatorHome.getAbsolutePath());
        System.setProperty("DRIVER","fake");
        return simulatorHome;
    }

    private static void copyDirectory(File source, File destination) throws IOException {
        if (source.isDirectory()) {
            if (!destination.exists()) {
                destination.mkdirs();
            }

            String fileNames[] = source.list();

            for (String fileName : fileNames) {
                File srcFile = new File(source, fileName);
                File destFile = new File(destination, fileName);
                copyDirectory(srcFile, destFile);
            }
        } else {
            FileUtils.copy(source,destination);

            if (source.canExecute()) {
                destination.setExecutable(true);
            }
        }
    }

    public static File setupFakeUserDir() {
        File dir = TestUtils.createTmpDirectory();
        System.setProperty("user.dir.test", dir.getAbsolutePath());
        return dir;
    }

    public static void teardownFakeUserDir() {
        System.clearProperty("user.dir.test");
    }

    public static void tearDownFakeEnvironment() {
        if (originalSimulatorHome == null) {
            System.clearProperty("SIMULATOR_HOME");
        } else {
            System.setProperty("SIMULATOR_HOME", originalSimulatorHome);
        }

        System.clearProperty("user.dir.test");
    }

    public static void setExitExceptionSecurityManagerWithStatusZero() {
        originalSecurityManager = System.getSecurityManager();
        System.setSecurityManager(new ExitExceptionSecurityManager(true));
    }

    public static void resetSecurityManager() {
        System.setSecurityManager(originalSecurityManager);
    }

    public static File internalDistDirectory() {
        File localResourceDir = localResourceDirectory();
        File projectRoot = localResourceDir.getParentFile().getParentFile().getParentFile();
        return new File(projectRoot, "dist/src/main/dist");
    }

    public static File localResourceDirectory() {
        ClassLoader classLoader = TestEnvironmentUtils.class.getClassLoader();
        File f = new File(classLoader.getResource("hazelcast.xml").getFile());
        return f.getParentFile().getAbsoluteFile();
    }

    public static void createAgentsFileWithLocalhost() {
        inventoryFile = new File(getUserDir(), "infrastructure.yaml");
//        String text = "nodes:\n"+
//                ""

        appendText("127.0.0.1", inventoryFile);
    }

}
