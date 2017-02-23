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
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static com.hazelcast.simulator.utils.CommonUtils.closeQuietly;
import static com.hazelcast.simulator.utils.CommonUtils.rethrow;
import static com.hazelcast.simulator.utils.FileUtils.fileAsText;
import static com.hazelcast.simulator.utils.FileUtils.getSimulatorHome;
import static com.hazelcast.simulator.utils.FileUtils.getUserDir;
import static com.hazelcast.simulator.utils.FileUtils.newFile;
import static java.lang.Float.parseFloat;
import static java.lang.Integer.parseInt;
import static java.lang.String.format;

/**
 * Loads the Hazelcast Simulator properties file.
 * <p>
 * This class will always load the properties in the <tt>${SIMULATOR_HOME}/conf/simulator.properties</tt> as defaults. If an
 * explicit {$value #PROPERTIES_FILE_NAME} file is configured or {$value #PROPERTIES_FILE_NAME} is available in the working dir,
 * it will override the properties from the default.
 */
@SuppressWarnings("checkstyle:methodcount")
public class SimulatorProperties {

    public static final String PROPERTIES_FILE_NAME = "simulator.properties";

    public static final String CLOUD_PROVIDER = "CLOUD_PROVIDER";
    public static final String CLOUD_IDENTITY = "CLOUD_IDENTITY";
    public static final String CLOUD_CREDENTIAL = "CLOUD_CREDENTIAL";

    private static final int WORKER_TIMEOUT_FACTOR = 3;

    private static final Logger LOGGER = Logger.getLogger(SimulatorProperties.class);

    private final Map<String, Value> properties = new HashMap<String, Value>();


    public SimulatorProperties() {
        File defaultPropFile = newFile(getSimulatorHome(), "conf", PROPERTIES_FILE_NAME);

        LOGGER.info(format("Loading default %s: %s", PROPERTIES_FILE_NAME, defaultPropFile.getAbsolutePath()));
        check(defaultPropFile);
        load(defaultPropFile, true);
    }

    public Map<String, String> asMap() {
        Map<String, String> map = new HashMap<String, String>();
        for (Map.Entry entry : properties.entrySet()) {
            String key = (String) entry.getKey();
            map.put(key, get(key));
        }
        return map;
    }

    /**
     * Initializes the SimulatorProperties with additional properties.
     *
     * @param file the file to load the properties from. If {@code null}, the {@value #PROPERTIES_FILE_NAME} file in the working
     *             directory is tried.
     * @throws CommandLineExitException if the given file does not exist. If file is {@code null} no exception will be thrown,
     *                                  even if the {@value #PROPERTIES_FILE_NAME} file in the working directory cannot be found.
     */
    public SimulatorProperties init(File file) {
        if (file == null) {
            // if no file is explicitly given, we look in the working directory
            file = new File(getUserDir(), PROPERTIES_FILE_NAME);
            if (!file.exists()) {
                LOGGER.info(format("Found no %s in working directory, relying on default properties", PROPERTIES_FILE_NAME));
                return null;
            }
        }

        LOGGER.info(format("Loading additional %s: %s", PROPERTIES_FILE_NAME, file.getAbsolutePath()));
        check(file);
        load(file, false);
        return this;
    }

    private void check(File file) {
        if (!file.exists()) {
            throw new CommandLineExitException(format("Could not find %s: %s", PROPERTIES_FILE_NAME, file.getAbsolutePath()));
        }
    }

    void load(File file) {
        load(file, false);
    }

    void load(File file, boolean isDefault) {
        FileInputStream in = null;
        try {
            in = new FileInputStream(file);

            Properties p = new Properties();
            p.load(in);

            for (Map.Entry entry : p.entrySet()) {
                String key = (String) entry.getKey();
                String value = (String) entry.getValue();
                properties.put(key, new Value(isDefault, value));
            }

            if (containsKey("HAZELCAST_VERSION_SPEC")) {
                throw new IOException("'HAZELCAST_VERSION_SPEC' property is deprecated, Use 'VERSION_SPEC' instead.");
            }
        } catch (IOException e) {
            throw rethrow(e);
        } finally {
            closeQuietly(in);
        }
    }

    public boolean containsKey(String name) {
        return properties.containsKey(name);
    }

    public String getSshOptions() {
        return get("SSH_OPTIONS", "");
    }

    public String getUser() {
        return get("SIMULATOR_USER", "simulator");
    }

    public String getJdkFlavor() {
        return get("JDK_FLAVOR", "outofthebox");
    }

    public String getJdkVersion() {
        return get("JDK_VERSION", "7");
    }

    public String getVersionSpec() {
        return get("VERSION_SPEC", "outofthebox");
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

    public int getWorkerStartupTimeoutSeconds() {
        return parseInt(get("WORKER_STARTUP_TIMEOUT_SECONDS", "60"));
    }

    public int getWaitForWorkerShutdownTimeoutSeconds() {
        return parseInt(get("WAIT_FOR_WORKER_SHUTDOWN_TIMEOUT_SECONDS", "120"));
    }

    public int getTestCompletionTimeoutSeconds() {
        return parseInt(get("TEST_COMPLETION_TIMEOUT_SECONDS", "300"));
    }

    public int getCoordinatorPort() {
        return parseInt(get("COORDINATOR_PORT", "0"));
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
        return get(CLOUD_PROVIDER);
    }

    public void setCloudProvider(String value) {
        set(CLOUD_PROVIDER, value);
    }

    public String getCloudIdentity() {
        return loadDirectOrFile(CLOUD_IDENTITY);
    }

    public String getCloudCredential() {
        return loadDirectOrFile(CLOUD_CREDENTIAL);
    }

    private String loadDirectOrFile(String property) {
        String value = get(property);
        if (value == null) {
            throw new IllegalArgumentException(format("Simulator property '%s' is not found", property));
        }

        File file = newFile(value);
        if (file.exists()) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(format("Loading simulator property value for %s from file: %s", property, file.getAbsolutePath()));
            }

            return fileAsText(file).trim();
        } else if (value.equals("~/ec2.identity") || value.equals("~/ec2.credential")) {
            throw new CommandLineExitException("file [" + value + "] is not found");
        } else {
            return value.trim();
        }
    }

    public String get(String name) {
        return get(name, null);
    }

    public String get(String name, String defaultValue) {
        Value value = properties.get(name);

        String result = null;

        // if a non default value has been set; that is being returned because highest priority
        if (value != null && !value.isDefault) {
            result = value.text;
        }

        // then we check system properties
        if (result == null) {
            result = System.getProperty(name);
        }

        // then we check environment
        // checking environment is useful for external configuration of simulator properties using exported variables
        if (result == null) {
            result = System.getenv(name);
        }

        // then we check the default value from the simulator.properties from the distribution
        if (result == null && value != null) {
            result = value.text;
        }

        // and eventually we default to the default value.
        if (result == null) {
            result = defaultValue;
        }

        return fixValue(name, result);
    }

    public void set(String name, String value) {
        properties.put(name, new Value(false, value));
    }

    public Float getAsFloat(String property) {
        String value = get(property);
        if (value == null) {
            return null;
        }
        return parseFloat(value);
    }

    public String getDefaultsAsString() {
        return fileAsText(newFile(getSimulatorHome(), "conf", PROPERTIES_FILE_NAME));
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

    private static final class Value {
        final boolean isDefault;
        final String text;

        private Value(boolean isDefault, String text) {
            this.isDefault = isDefault;
            this.text = text;
        }
    }
}
