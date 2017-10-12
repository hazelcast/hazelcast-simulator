/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hazelcast.simulator;

import com.hazelcast.simulator.common.AgentsFile;
import com.hazelcast.simulator.utils.FileUtils;
import com.hazelcast.simulator.utils.TestUtils;
import com.hazelcast.simulator.utils.helper.ExitExceptionSecurityManager;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.File;
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.simulator.utils.FileUtils.USER_HOME;
import static com.hazelcast.simulator.utils.FileUtils.appendText;
import static com.hazelcast.simulator.utils.FileUtils.deleteQuiet;
import static com.hazelcast.simulator.utils.FileUtils.ensureExistingFile;
import static com.hazelcast.simulator.utils.FileUtils.getUserDir;
import static com.hazelcast.simulator.utils.FileUtils.newFile;

@SuppressWarnings("unused")
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
        FileUtils.copyDirectory(dist, simulatorHome);

        System.setProperty("user.dir.test", simulatorHome.getAbsolutePath());

        originalSimulatorHome = System.getProperty("SIMULATOR_HOME");
        System.setProperty("SIMULATOR_HOME", simulatorHome.getAbsolutePath());

        return simulatorHome;
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

        String userDir = System.clearProperty("user.dir.test");
        if (userDir != null) {
            deleteQuiet(userDir);
        }
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
