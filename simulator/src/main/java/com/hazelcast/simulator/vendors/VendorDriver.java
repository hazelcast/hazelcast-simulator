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
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static com.hazelcast.simulator.utils.FileUtils.fileAsText;
import static com.hazelcast.simulator.utils.FileUtils.getFileAsTextFromWorkingDirOrBaseDir;
import static com.hazelcast.simulator.utils.FileUtils.getSimulatorHome;
import static java.lang.String.format;

public abstract class VendorDriver<V> implements Closeable {
    private static final Logger LOGGER = Logger.getLogger(HazelcastDriver.class);

    protected String clientArgs = "";
    protected String memberArgs = "";
    protected List<AgentData> agents;
    protected Map<String, String> properties = new HashMap<String, String>();

    public static VendorDriver newVendorDriver(String vendorName) {
        LOGGER.info(format("Loading vendor [%s]", vendorName));

        if (vendorName.equals("hazelcast") || vendorName.equals("hazelcast-enterprise")) {
            return new HazelcastDriver();
        } else if (vendorName.equals("ignite")) {
            return new IgniteDriver();
        } else {
            throw new IllegalArgumentException("Unknown vendor [" + vendorName + "]");
        }
    }

    public VendorDriver<V> setAgents(List<AgentData> agents) {
        this.agents = agents;
        return this;
    }

    public VendorDriver<V> setClientArgs(String clientArgs) {
        this.clientArgs = clientArgs;
        return this;
    }

    public VendorDriver<V> setMemberArgs(String memberArgs) {
        this.memberArgs = memberArgs;
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

    public abstract V getInstance();

    public abstract void createVendorInstance() throws Exception;

    public abstract WorkerParameters loadWorkerParameters(String workerType);

    public static String loadLog4jConfig() {
        return getFileAsTextFromWorkingDirOrBaseDir(
                getSimulatorHome(), "worker-log4j.xml", "Log4j configuration for Worker");
    }

    public static String loadWorkerScript(String workerType, String vendor) {
        List<File> files = new LinkedList<File>();
        File confDir = new File(getSimulatorHome(), "conf");

        files.add(new File("worker-" + vendor + "-" + workerType + ".sh").getAbsoluteFile());
        files.add(new File("worker-" + workerType + ".sh").getAbsoluteFile());
        files.add(new File("worker-" + vendor + ".sh").getAbsoluteFile());
        files.add(new File("worker.sh").getAbsoluteFile());

        files.add(new File(confDir, "worker-" + vendor + "-" + workerType + ".sh").getAbsoluteFile());
        files.add(new File(confDir, "worker-" + vendor + ".sh").getAbsoluteFile());
        files.add(new File(confDir, "worker.sh").getAbsoluteFile());

        for (File file : files) {
            if (file.exists()) {
                LOGGER.info("Loading " + vendor + " " + workerType + " worker script: " + file.getAbsolutePath());
                return fileAsText(file);
            }
        }

        throw new CommandLineExitException("Failed to load worker script from the following locations:" + files);
    }
}
