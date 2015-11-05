package com.hazelcast.simulator;

import com.hazelcast.simulator.common.AgentsFile;
import com.hazelcast.simulator.utils.helper.ExitExceptionSecurityManager;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.File;
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.simulator.utils.FileUtils.USER_HOME;
import static com.hazelcast.simulator.utils.FileUtils.appendText;
import static com.hazelcast.simulator.utils.FileUtils.deleteQuiet;
import static com.hazelcast.simulator.utils.FileUtils.ensureExistingFile;
import static com.hazelcast.simulator.utils.FileUtils.getSimulatorHome;
import static com.hazelcast.simulator.utils.FileUtils.newFile;

public class TestEnvironmentUtils {

    private static final Logger LOGGER = Logger.getLogger(TestEnvironmentUtils.class);
    private static final Logger ROOT_LOGGER = Logger.getRootLogger();
    private static final AtomicReference<Level> LOGGER_LEVEL = new AtomicReference<Level>();

    private static SecurityManager originalSecurityManager;

    private static String originalUserDir;

    private static File agentsFile;

    private static File cloudIdentity;
    private static File cloudCredential;

    private static File publicKeyFile;
    private static File privateKeyFile;

    private static boolean deleteCloudIdentity;
    private static boolean deleteCloudCredential;

    private static boolean deletePublicKey;
    private static boolean deletePrivateKey;

    public static void setLogLevel(Level level) {
        if (LOGGER_LEVEL.compareAndSet(null, ROOT_LOGGER.getLevel())) {
            ROOT_LOGGER.setLevel(level);
        }
    }

    public static void resetLogLevel() {
        Level level = LOGGER_LEVEL.get();
        if (level != null && LOGGER_LEVEL.compareAndSet(level, null)) {
            ROOT_LOGGER.setLevel(level);
        }
    }

    public static void setExitExceptionSecurityManager() {
        originalSecurityManager = System.getSecurityManager();
        System.setSecurityManager(new ExitExceptionSecurityManager());
    }

    public static void setExitExceptionSecurityManagerWithStatusZero() {
        originalSecurityManager = System.getSecurityManager();
        System.setSecurityManager(new ExitExceptionSecurityManager(true));
    }

    public static void resetSecurityManager() {
        System.setSecurityManager(originalSecurityManager);
    }

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

    public static void createAgentsFileWithLocalhost() {
        agentsFile = new File(AgentsFile.NAME);
        appendText("127.0.0.1", agentsFile);
    }

    public static void deleteAgentsFile() {
        deleteQuiet(agentsFile);
    }

    public static void createCloudCredentialFiles() {
        String userHome = USER_HOME;
        cloudIdentity = new File(userHome, "ec2.identity").getAbsoluteFile();
        cloudCredential = new File(userHome, "ec2.credential").getAbsoluteFile();

        if (!cloudIdentity.exists()) {
            deleteCloudIdentity = true;
            ensureExistingFile(cloudIdentity);
        }
        if (!cloudCredential.exists()) {
            deleteCloudCredential = true;
            ensureExistingFile(cloudCredential);
        }
    }

    public static void deleteCloudCredentialFiles() {
        if (deleteCloudIdentity) {
            deleteQuiet(cloudIdentity);
        }
        if (deleteCloudCredential) {
            deleteQuiet(cloudCredential);
        }
    }

    public static void createPublicPrivateKeyFiles() {
        publicKeyFile = newFile("~", ".ssh", "id_rsa.pub");
        privateKeyFile = newFile("~", ".ssh", "id_rsa");

        if (!publicKeyFile.exists()) {
            deletePublicKey = true;
            ensureExistingFile(publicKeyFile);
        }
        if (!privateKeyFile.exists()) {
            deletePrivateKey = true;
            ensureExistingFile(privateKeyFile);
        }
    }

    public static void deletePublicPrivateKeyFiles() {
        if (deletePublicKey) {
            deleteQuiet(publicKeyFile);
        }
        if (deletePrivateKey) {
            deleteQuiet(privateKeyFile);
        }
    }

    public static File getPublicKeyFile() {
        return publicKeyFile;
    }

    public static File getPrivateKeyFile() {
        return privateKeyFile;
    }
}
