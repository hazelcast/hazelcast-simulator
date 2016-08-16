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
package com.hazelcast.simulator.provisioner;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.elasticloadbalancing.AmazonElasticLoadBalancingClient;
import com.hazelcast.simulator.common.AgentsFile;
import com.hazelcast.simulator.common.SimulatorProperties;
import com.hazelcast.simulator.protocol.registry.ComponentRegistry;
import com.hazelcast.simulator.utils.CommandLineExitException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.apache.log4j.Logger;

import java.io.File;

import static com.hazelcast.simulator.common.GitInfo.getBuildTime;
import static com.hazelcast.simulator.common.GitInfo.getCommitIdAbbrev;
import static com.hazelcast.simulator.common.SimulatorProperties.PROPERTIES_FILE_NAME;
import static com.hazelcast.simulator.utils.CliUtils.initOptionsWithHelp;
import static com.hazelcast.simulator.utils.CliUtils.printHelpAndExit;
import static com.hazelcast.simulator.utils.CommonUtils.exitWithError;
import static com.hazelcast.simulator.utils.CommonUtils.getSimulatorVersion;
import static com.hazelcast.simulator.utils.FileUtils.getSimulatorHome;
import static com.hazelcast.simulator.utils.SimulatorUtils.loadComponentRegister;
import static com.hazelcast.simulator.utils.SimulatorUtils.loadSimulatorProperties;
import static java.lang.String.format;

final class AwsProvisionerCli {
    private static final Logger LOGGER = Logger.getLogger(AwsProvisionerCli.class);

    AwsProvisioner provisioner;

    private final OptionParser parser = new OptionParser();

    private final OptionSpec<String> propertiesFileSpec = parser.accepts("propertiesFile",
            format("The file containing the simulator properties. If no file is explicitly configured,"
                            + " first the working directory is checked for a file '%s'."
                            + " All missing properties are always loaded from SIMULATOR_HOME/conf/%s",
                    PROPERTIES_FILE_NAME, PROPERTIES_FILE_NAME))
            .withRequiredArg().ofType(String.class);

    private final OptionSpec<Integer> scaleSpec = parser.accepts("scale",
            "Desired number of machines to scale to.")
            .withRequiredArg().ofType(Integer.class);

    private final OptionSpec<String> createLoadBalancerSpec = parser.accepts("newLb",
            "Create new load balancer if it dose not exist.")
            .withRequiredArg().ofType(String.class);

    private final OptionSpec<String> addAgentsToLoadBalancer = parser.accepts("addToLb",
            "Adds the IP addresses in '" + AgentsFile.NAME + "' file to the load balancer.")
            .withRequiredArg().ofType(String.class);
    private final OptionSet options;

    AwsProvisionerCli(String[] args) {
        options = initOptionsWithHelp(parser, args);

        SimulatorProperties properties = loadSimulatorProperties(options, propertiesFileSpec);
        ComponentRegistry componentRegistry = loadComponentRegister(new File(AgentsFile.NAME), false);

        try {
            String awsCredentialsPath = properties.get("AWS_CREDENTIALS", "awscredentials.properties");
            File credentialsFile = new File(awsCredentialsPath);
            AWSCredentials credentials = new PropertiesCredentials(credentialsFile);
            AmazonEC2 ec2 = new AmazonEC2Client(credentials);
            AmazonElasticLoadBalancingClient elb = new AmazonElasticLoadBalancingClient(credentials);

            provisioner = new AwsProvisioner(ec2, elb, componentRegistry, properties);
        } catch (Exception e) {
            throw new CommandLineExitException("Credentials file could not be loaded", e);
        }
    }

    void run() {
        try {
            if (options.has(scaleSpec)) {
                int count = options.valueOf(scaleSpec);
                provisioner.scaleInstanceCountTo(count);
            } else if (options.has(createLoadBalancerSpec)) {
                String name = options.valueOf(createLoadBalancerSpec);
                provisioner.createLoadBalancer(name);
            } else if (options.has(addAgentsToLoadBalancer)) {
                String name = options.valueOf(addAgentsToLoadBalancer);
                provisioner.addAgentsToLoadBalancer(name);
            } else {
                printHelpAndExit(parser);
            }
        } finally {
            provisioner.shutdown();
        }
    }

    public static void main(String[] args) {
        LOGGER.info("Hazelcast Simulator AWS Provisioner");
        LOGGER.info(format("Version: %s, Commit: %s, Build Time: %s",
                getSimulatorVersion(), getCommitIdAbbrev(), getBuildTime()));
        LOGGER.info(format("SIMULATOR_HOME: %s", getSimulatorHome().getAbsolutePath()));

        try {
            AwsProvisionerCli cli = new AwsProvisionerCli(args);
            cli.run();
        } catch (Exception e) {
            exitWithError(LOGGER, "Could not provision machines", e);
        }
    }
}
