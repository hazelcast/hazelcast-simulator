/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast.simulator.provisioner;

import com.hazelcast.simulator.common.AgentsFile;
import com.hazelcast.simulator.common.SimulatorProperties;
import com.hazelcast.simulator.utils.Bash;
import com.hazelcast.simulator.utils.jars.HazelcastJARs;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.jclouds.compute.ComputeService;

import static com.hazelcast.simulator.common.SimulatorProperties.PROPERTIES_FILE_NAME;
import static com.hazelcast.simulator.utils.CliUtils.initOptionsWithHelp;
import static com.hazelcast.simulator.utils.CliUtils.printHelpAndExit;
import static com.hazelcast.simulator.utils.CloudProviderUtils.isStatic;
import static com.hazelcast.simulator.utils.SimulatorUtils.loadSimulatorProperties;
import static com.hazelcast.simulator.utils.jars.HazelcastJARs.isPrepareRequired;
import static com.hazelcast.simulator.utils.jars.HazelcastJARs.newInstance;
import static java.util.Collections.singleton;

final class ProvisionerCli {

    private final OptionParser parser = new OptionParser();

    private final OptionSpec<Integer> scaleSpec = parser.accepts("scale",
            "Number of Simulator machines to scale to. If the number of machines already exists, the call is ignored. If the"
                    + " desired number of machines is smaller than the actual number of machines, machines are terminated.")
            .withRequiredArg().ofType(Integer.class);

    private final OptionSpec installSpec = parser.accepts("install",
            "Installs Simulator on all provisioned machines.");

    private final OptionSpec uploadHazelcastSpec = parser.accepts("uploadHazelcast",
            "If defined --install will upload the Hazelcast JARs as well.");

    private final OptionSpec<Boolean> enterpriseEnabledSpec = parser.accepts("enterpriseEnabled",
            "Use JARs of Hazelcast Enterprise Edition.")
            .withRequiredArg().ofType(Boolean.class).defaultsTo(false);

    private final OptionSpec listAgentsSpec = parser.accepts("list",
            "Lists the provisioned machines (from " + AgentsFile.NAME + " file).");

    private final OptionSpec<String> downloadSpec = parser.accepts("download",
            "Download all files from the remote Worker directories. Use --clean to delete all Worker directories.")
            .withOptionalArg().ofType(String.class).defaultsTo("workers");

    private final OptionSpec cleanSpec = parser.accepts("clean",
            "Cleans the remote Worker directories on the provisioned machines.");

    private final OptionSpec killSpec = parser.accepts("kill",
            "Kills the Java processes on all provisioned machines (via killall -9 java).");

    private final OptionSpec terminateSpec = parser.accepts("terminate",
            "Terminates all provisioned machines.");

    private final OptionSpec<String> propertiesFileSpec = parser.accepts("propertiesFile",
            "The file containing the Simulator properties. If no file is explicitly configured, first the local working directory"
                    + " is checked for a file '" + PROPERTIES_FILE_NAME + "'. All missing properties are always loaded from"
                    + " '$SIMULATOR_HOME/conf/" + PROPERTIES_FILE_NAME + "'.")
            .withRequiredArg().ofType(String.class);

    private ProvisionerCli() {
    }

    static Provisioner init(String[] args) {
        ProvisionerCli cli = new ProvisionerCli();
        OptionSet options = initOptionsWithHelp(cli.parser, args);

        SimulatorProperties properties = loadSimulatorProperties(options, cli.propertiesFileSpec);
        ComputeService computeService = isStatic(properties) ? null : new ComputeServiceBuilder(properties).build();
        Bash bash = new Bash(properties);

        HazelcastJARs hazelcastJARs = null;
        boolean enterpriseEnabled = options.valueOf(cli.enterpriseEnabledSpec);
        if (options.has(cli.uploadHazelcastSpec)) {
            String hazelcastVersionSpec = properties.getHazelcastVersionSpec();
            if (isPrepareRequired(hazelcastVersionSpec) || !enterpriseEnabled) {
                hazelcastJARs = newInstance(bash, properties, singleton(hazelcastVersionSpec));
            }
        }

        return new Provisioner(properties, computeService, bash, hazelcastJARs, enterpriseEnabled);
    }

    static void run(String[] args, Provisioner provisioner) {
        ProvisionerCli cli = new ProvisionerCli();
        OptionSet options = initOptionsWithHelp(cli.parser, args);

        try {
            if (options.has(cli.scaleSpec)) {
                int size = options.valueOf(cli.scaleSpec);
                provisioner.scale(size);
            } else if (options.has(cli.installSpec)) {
                provisioner.installSimulator();
            } else if (options.has(cli.listAgentsSpec)) {
                provisioner.listMachines();
            } else if (options.has(cli.downloadSpec)) {
                String dir = options.valueOf(cli.downloadSpec);
                provisioner.download(dir);
            } else if (options.has(cli.cleanSpec)) {
                provisioner.clean();
            } else if (options.has(cli.killSpec)) {
                provisioner.killJavaProcesses();
            } else if (options.has(cli.terminateSpec)) {
                provisioner.terminate();
            } else {
                printHelpAndExit(cli.parser);
            }
        } finally {
            provisioner.shutdown();
        }
    }
}
