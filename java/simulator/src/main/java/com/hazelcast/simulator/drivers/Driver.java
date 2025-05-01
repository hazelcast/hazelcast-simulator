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
package com.hazelcast.simulator.drivers;

import com.hazelcast.simulator.coordinator.registry.AgentData;
import com.hazelcast.simulator.utils.CommandLineExitException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.lang.String.format;

public abstract class Driver<V> implements Closeable {
    private static final Logger LOGGER = LogManager.getLogger(Driver.class);

    protected List<AgentData> agents;
    protected Map<String, String> properties = new HashMap<>();
    private final Map<String, String> configCache = new HashMap<>();

    public static Driver loadDriver(String driverClass) {
        LOGGER.info("Loading driver [{}]", driverClass);
        return loadInstance(driverClass);
    }

    private static Driver loadInstance(String driverClassName) {
        Class driverClass;
        try {
            driverClass = Driver.class.getClassLoader().loadClass(driverClassName);
        } catch (ClassNotFoundException e) {
            throw new CommandLineExitException(format("Could not locate driver class [%s]", driverClassName));
        }

        try {
            return (Driver) driverClass.newInstance();
        } catch (Exception e) {
            throw new CommandLineExitException(format("Failed to create an instance of driver [%s]", driverClassName), e);
        }
    }

    /**
     * This method closes any active driver instance. Method is called on the worker-side.
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
    public Driver<V> setAgents(List<AgentData> agents) {
        this.agents = agents;
        return this;
    }

    public Driver<V> setAll(Map<String, String> properties) {
        this.properties.putAll(properties);
        return this;
    }

    public Driver<V> set(String key, Object value) {
        properties.put(key, "" + value);
        return this;
    }

    /**
     * Gets the created Driver instance. Method is called on the worker-side
     *
     * @return the created driver.
     */
    public abstract V getDriverInstance();

    /**
     * Starts a Vendor instance. Method is called on the worker-side
     *
     * @throws Exception when something fails starting the driver instance. Checked exception to prevent forcing to handle
     *                   exceptions
     */
    public abstract void startDriverInstance() throws Exception;



}
