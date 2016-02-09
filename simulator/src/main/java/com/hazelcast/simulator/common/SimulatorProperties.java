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
package com.hazelcast.simulator.common;

import com.hazelcast.simulator.utils.CommandLineExitException;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import static com.hazelcast.simulator.utils.CommonUtils.closeQuietly;
import static com.hazelcast.simulator.utils.CommonUtils.rethrow;
import static com.hazelcast.simulator.utils.FileUtils.fileAsText;
import static com.hazelcast.simulator.utils.FileUtils.getSimulatorHome;
import static com.hazelcast.simulator.utils.FileUtils.newFile;
import static com.hazelcast.simulator.utils.jars.HazelcastJARs.GIT_VERSION_PREFIX;
import static com.hazelcast.simulator.utils.jars.HazelcastJARs.OUT_OF_THE_BOX;
import static java.lang.Integer.parseInt;
import static java.lang.String.format;

/**
 * Loads the Hazelcast Simulator properties file.
 *
 * This class will always load the properties in the <tt>${SIMULATOR_HOME}/conf/simulator.properties</tt> as defaults. If an
 * explicit {$value #PROPERTIES_FILE_NAME} file is configured or {$value #PROPERTIES_FILE_NAME} is available in the working dir,
 * it will override the properties from the default.
 */
public class SimulatorProperties {

    public static final String PROPERTIES_FILE_NAME = "simulator.properties";

    private static final int WORKER_TIMEOUT_FACTOR = 3;

    private static final Logger LOGGER = Logger.getLogger(SimulatorProperties.class);

    private final Properties properties = new Properties();

    private String forcedHazelcastVersionSpec;

    public SimulatorProperties() {
        File defaultPropertiesFile = newFile(getSimulatorHome(), "conf", PROPERTIES_FILE_NAME);
        LOGGER.debug(format("Loading default %s from: %s", PROPERTIES_FILE_NAME, defaultPropertiesFile.getAbsolutePath()));
        check(defaultPropertiesFile);
        load(defaultPropertiesFile);
    }

    /**
     * Initializes the SimulatorProperties.
     *
     * @param file the file to load the properties from. If the file is <code>null</code>,
     *             then first the simulator.properties in the working directory is checked.
     */
    public void init(File file) {
        if (file == null) {
            // if no file is explicitly given, we look in the working directory
            file = new File(PROPERTIES_FILE_NAME);
            if (!file.exists()) {
                LOGGER.warn(format("%s is not found, relying on defaults", file.getAbsolutePath()));
                return;
            }
        }

        LOGGER.info(format("Loading %s: %s", PROPERTIES_FILE_NAME, file.getAbsolutePath()));
        check(file);
        load(file);
    }

    private void check(File file) {
        if (!file.exists()) {
            throw new CommandLineExitException(
                    format("Could not find %s file: %s", PROPERTIES_FILE_NAME, file.getAbsolutePath()));
        }
    }

    void load(File file) {
        FileInputStream inputStream = null;
        try {
            inputStream = new FileInputStream(file);
            properties.load(inputStream);
        } catch (IOException e) {
            throw rethrow(e);
        } finally {
            closeQuietly(inputStream);
        }
    }

    public void forceGit(String gitRevision) {
        if (gitRevision != null && !gitRevision.isEmpty()) {
            forcedHazelcastVersionSpec = GIT_VERSION_PREFIX + gitRevision;
            LOGGER.info("Overriding Hazelcast version to Git revision " + gitRevision);
        }
    }

    public String getSshOptions() {
        return get("SSH_OPTIONS", "");
    }

    public String getUser() {
        return get("USER", "simulator");
    }

    public String getHazelcastVersionSpec() {
        if (forcedHazelcastVersionSpec == null) {
            return get("HAZELCAST_VERSION_SPEC", OUT_OF_THE_BOX);
        } else {
            return forcedHazelcastVersionSpec;
        }
    }

    public int getWorkerPingIntervalSeconds() {
        return parseInt(get("WORKER_PING_INTERVAL_SECONDS", "60"));
    }

    public int getWorkerLastSeenTimeoutSeconds() {
        return getWorkerPingIntervalSeconds() * WORKER_TIMEOUT_FACTOR;
    }

    public int getMemberWorkerShutdownDelaySeconds() {
        return parseInt(get("MEMBER_WORKER_SHUTDOWN_DELAY_SECONDS", "5"));
    }

    public int getWaitForWorkerShutdownTimeoutSeconds() {
        return parseInt(get("WAIT_FOR_WORKER_SHUTDOWN_TIMEOUT_SECONDS", "120"));
    }

    public int getAgentThreadPoolSize() {
        return parseInt(get("AGENT_THREAD_POOL_SIZE", "0"));
    }

    public int getAgentPort() {
        return parseInt(get("AGENT_PORT", "9000"));
    }

    public int getHazelcastPort() {
        return parseInt(get("HAZELCAST_PORT", "5701"));
    }

    public int getHazelcastPortRangeSize() {
        return parseInt(get("HAZELCAST_PORT_RANGE_SIZE", "50"));
    }

    public String getCloudProvider() {
        return get("CLOUD_PROVIDER");
    }

    public String getCloudIdentity() {
        return get("CLOUD_IDENTITY");
    }

    public String getCloudCredential() {
        return get("CLOUD_CREDENTIAL");
    }

    public String get(String name) {
        String value = (String) properties.get(name);

        if ("CLOUD_IDENTITY".equals(name)) {
            value = loadPropertyFromFile("CLOUD_IDENTITY", value);
        } else if ("CLOUD_CREDENTIAL".equals(name)) {
            value = loadPropertyFromFile("CLOUD_CREDENTIAL", value);
        }

        return fixValue(name, value);
    }

    public String get(String name, String defaultValue) {
        String value = System.getProperty(name);
        if (value != null) {
            return value;
        }

        value = (String) properties.get(name);
        if (value == null) {
            value = defaultValue;
        }

        return fixValue(name, value);
    }

    public void set(String name, String value) {
        properties.setProperty(name, value);
    }

    private String loadPropertyFromFile(String property, String path) {
        File file = newFile(path);
        if (!file.exists()) {
            throw new CommandLineExitException(format("Can't find property %s file %s", property, path));
        }

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Loading " + property + " from file: " + file.getAbsolutePath());
        }
        return fileAsText(file).trim();
    }

    private String fixValue(String name, String value) {
        if (value == null) {
            return null;
        }

        if ("GROUP_NAME".equals(name)) {
            String username = System.getProperty("user.name").toLowerCase();

            StringBuilder fixedUserName = new StringBuilder();
            for (char character : username.toCharArray()) {
                if (Character.isLetter(character) || Character.isDigit(character)) {
                    fixedUserName.append(character);
                }
            }

            return value.replace("${username}", fixedUserName.toString());
        }

        return value;
    }
}
