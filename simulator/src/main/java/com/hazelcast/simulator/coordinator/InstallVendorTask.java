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
package com.hazelcast.simulator.coordinator;

import com.hazelcast.simulator.common.SimulatorProperties;
import com.hazelcast.simulator.common.TestSuite;
import com.hazelcast.simulator.utils.BashCommand;
import org.apache.log4j.Logger;

import java.util.Set;

import static com.hazelcast.simulator.utils.CloudProviderUtils.isLocal;
import static com.hazelcast.simulator.utils.FileUtils.getConfigurationFile;
import static com.hazelcast.simulator.utils.FormatUtils.join;

/**
 * Install the software for a given vendor on a set of remote machines.
 */
public class InstallVendorTask {

    private static final Logger LOGGER = Logger.getLogger(InstallVendorTask.class);

    private final SimulatorProperties simulatorProperties;
    private final Set<String> publicIps;
    private final Set<String> versionSpecs;

    // todo: the placement of testSuite here is no good. Insallation will be done before
    // a test suite is even selected.
    private final TestSuite testSuite;

    public InstallVendorTask(SimulatorProperties simulatorProperties,
                             Set<String> publicIps,
                             Set<String> versionSpecs,
                             TestSuite testSuite) {
        this.simulatorProperties = simulatorProperties;
        this.publicIps = publicIps;
        this.versionSpecs = versionSpecs;
        this.testSuite = testSuite;
    }

    public void run() {
        String ipString = "";
        if (!isLocal(simulatorProperties)) {
            ipString = join(publicIps, ",");
        }

        String vendor = simulatorProperties.get("VENDOR");
        String installFile = getConfigurationFile("install-" + vendor + ".sh").getPath();

        for (String versionSpec : versionSpecs) {
            LOGGER.info("Installing '" + vendor + "' version '" + versionSpec + "' on Agents using " + installFile);

            new BashCommand(installFile)
                    .addParams(testSuite.getId(), versionSpec, ipString)
                    .addParams()
                    .addEnvironment(simulatorProperties.asMap())
                    .execute();

            LOGGER.info("Successfully installed '" + vendor + "'");
        }
    }
}
