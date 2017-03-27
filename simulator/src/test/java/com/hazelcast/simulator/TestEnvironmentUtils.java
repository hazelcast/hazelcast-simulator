package com.hazelcast.simulator;

import com.hazelcast.simulator.common.AgentsFile;
import com.hazelcast.simulator.utils.FileUtils;
import com.hazelcast.simulator.utils.TestUtils;
import com.hazelcast.simulator.utils.UncheckedIOException;
import com.hazelcast.simulator.utils.helper.ExitExceptionSecurityManager;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.simulator.utils.CommonUtils.closeQuietly;
import static com.hazelcast.simulator.utils.FileUtils.USER_HOME;
import static com.hazelcast.simulator.utils.FileUtils.appendText;
import static com.hazelcast.simulator.utils.FileUtils.deleteQuiet;
import static com.hazelcast.simulator.utils.FileUtils.ensureExistingFile;
import static com.hazelcast.simulator.utils.FileUtils.getUserDir;
import static com.hazelcast.simulator.utils.FileUtils.newFile;

public class TestEnvironmentUtils {

    private static final Logger LOGGER = Logger.getLogger(TestEnvironmentUtils.class);
    private static final Logger ROOT_LOGGER = Logger.getRootLogger();
    private static final AtomicReference<Level> LOGGER_LEVEL = new AtomicReference<Level>();

    private static SecurityManager originalSecurityManager;

    private static String originalSimulatorHome;

    private static File agentsFile;

    private static File cloudIdentity;
    private static File cloudCredential;

    private static File publicKeyFile;
    private static File privateKeyFile;

    private static boolean deleteCloudIdentity;
    private static boolean deleteCloudCredential;

    private static boolean deletePublicKey;
    private static boolean deletePrivateKey;

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

    public static File internalDistDirectory() {
        File localResourceDir = localResourceDirectory();
        File projectRoot = localResourceDir.getParentFile().getParentFile().getParentFile();
        return new File(projectRoot, "dist/src/main/dist");
    }

    public static String internalDistPath() {
        return internalDistDirectory().getAbsolutePath();
    }

    public static File localResourceDirectory() {
        ClassLoader classLoader = TestEnvironmentUtils.class.getClassLoader();
        File f = new File(classLoader.getResource("hazelcast.xml").getFile());
        return f.getParentFile().getAbsoluteFile();
    }

    public static void createAgentsFileWithLocalhost() {
        agentsFile = new File(getUserDir(), AgentsFile.NAME);
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
