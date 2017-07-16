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
package com.hazelcast.simulator.vendors;

import com.hazelcast.simulator.agent.workerprocess.WorkerParameters;
import com.hazelcast.simulator.coordinator.registry.AgentData;
import com.hazelcast.simulator.utils.CommandLineExitException;
import org.apache.log4j.Logger;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static com.hazelcast.simulator.utils.FileUtils.fileAsText;
import static com.hazelcast.simulator.utils.FileUtils.getConfigurationFile;
import static com.hazelcast.simulator.utils.FileUtils.getSimulatorHome;
import static java.lang.String.format;

public abstract class VendorDriver<V> implements Closeable {
    private static final Logger LOGGER = Logger.getLogger(VendorDriver.class);

    protected List<AgentData> agents;
    protected Map<String, String> properties = new HashMap<String, String>();
    private final Map<String, String> configCache = new HashMap<String, String>();

    public static VendorDriver loadVendorDriver(String vendorName) {
        LOGGER.info(format("Loading vendor-driver [%s]", vendorName));

        if (vendorName.equals("hazelcast") || vendorName.equals("hazelcast-enterprise")) {
            return new HazelcastDriver();
        } else {
            String driverName = "com.hazelcast.simulator." + vendorName + "."
                    + vendorName.substring(0, 1).toUpperCase() + vendorName.substring(1) + "Driver";
            Class driverClass;
            try {
                driverClass = VendorDriver.class.getClassLoader().loadClass(driverName);
            } catch (ClassNotFoundException e) {
                throw new CommandLineExitException(format("Could not locate driver class [%s]", driverName));
            }

            try {
                return (VendorDriver) driverClass.newInstance();
            } catch (Exception e) {
                throw new CommandLineExitException(format("Failed to create an instance of driver [%s]", driverName), e);
            }
        }
    }

    /**
     * Installs the software on the agent machines. Method is called on the coordinator-side.
     */
    public void install() {
    }

    /**
     * This method closes any active vendor instance. Method is called on the worker-side.
     */
    @Override
    public void close() throws IOException {
    }

    protected String get(String name, String defaultValue) {
        String value = properties.get(name);
        return value == null ? defaultValue : value;
    }

    protected String get(String name) {
        return properties.get(name);
    }

    /**
     * Sets the agents to be used by the VendorDriver. Method is called on the coordinator-side.
     *
     * @param agents the agents
     * @return this
     */
    public VendorDriver<V> setAgents(List<AgentData> agents) {
        this.agents = agents;
        return this;
    }

    public VendorDriver<V> setAll(Map<String, String> properties) {
        this.properties.putAll(properties);
        return this;
    }

    public VendorDriver<V> set(String key, Object value) {
        properties.put(key, "" + value);
        return this;
    }

    public VendorDriver<V> setIfNotNull(String key, Object value) {
        if (value == null) {
            return this;
        }
        return set(key, value);
    }

    /**
     * Gets the created Vendor instance. Method is called on the worker-side
     *
     * @return the created vendor driver.
     */
    public abstract V getVendorInstance();

    /**
     * Starts a Vendor instance. Method is called on the worker-side
     *
     * @throws Exception when something fails starting the vendor instance. Checked exception to prevent forcing to handle
     *                   exceptions
     */
    public abstract void startVendorInstance() throws Exception;

    /**
     * Loads the parameters to create a worker. Method is called on the coordinator-side
     *
     * @param workerType the type of the worker.
     * @param agentIndex the index of the agent
     * @return the loaded WorkerParameters
     */
    public abstract WorkerParameters loadWorkerParameters(String workerType, int agentIndex);

    protected String loadConfiguration(String logPrefix, String filename) {
        File file = getConfigurationFile(filename);
        String config = configCache.get(filename);
        if (config == null) {
            config = fileAsText(file);
            configCache.put(filename, config);
            LOGGER.info("Loading " + logPrefix + ":" + file.getAbsolutePath());
        }
        return config;
    }

    protected String loadLog4jConfig() {
        return loadConfiguration("Log4J configuration for worker", "worker-log4j.xml");
    }

    protected String loadWorkerScript(String workerType) {
        List<File> files = new LinkedList<File>();
        File confDir = new File(getSimulatorHome(), "conf");

        String vendor = properties.get("VENDOR");

        files.add(new File("worker-" + vendor + "-" + workerType + ".sh").getAbsoluteFile());
        files.add(new File("worker-" + workerType + ".sh").getAbsoluteFile());
        files.add(new File("worker-" + vendor + ".sh").getAbsoluteFile());
        files.add(new File("worker.sh").getAbsoluteFile());

        files.add(new File(confDir, "worker-" + vendor + "-" + workerType + ".sh").getAbsoluteFile());
        files.add(new File(confDir, "worker-" + vendor + ".sh").getAbsoluteFile());
        files.add(new File(confDir, "worker.sh").getAbsoluteFile());

        for (File file : files) {
            if (file.exists()) {
                String key = workerType + "#" + file.getName();
                String config = configCache.get(key);
                if (config == null) {
                    config = fileAsText(file);
                    configCache.put(key, config);
                    LOGGER.info("Loading " + vendor + " " + workerType + " worker script: " + file.getAbsolutePath());
                }
                return config;
            }
        }

        throw new CommandLineExitException("Failed to load worker script from the following locations:" + files);
    }
}
