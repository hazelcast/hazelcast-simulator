/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

import java.io.File;

import static com.hazelcast.simulator.utils.CliUtils.initOptionsWithHelp;
import static com.hazelcast.simulator.utils.CliUtils.printHelpAndExit;
import static com.hazelcast.simulator.utils.FileUtils.getUserDir;
import static com.hazelcast.simulator.utils.SimulatorUtils.loadComponentRegister;
import static com.hazelcast.simulator.utils.SimulatorUtils.loadSimulatorProperties;

final class AwsProvisionerCli {

    private final OptionParser parser = new OptionParser();

    private final OptionSpec<Integer> scaleSpec = parser.accepts("scale",
            "Desired number of machines to scale to.")
            .withRequiredArg().ofType(Integer.class);

    private final OptionSpec<String> createLoadBalancerSpec = parser.accepts("newLb",
            "Create new load balancer if it dose not exist.")
            .withRequiredArg().ofType(String.class);

    private final OptionSpec<String> addAgentsToLoadBalancer = parser.accepts("addToLb",
            "Adds the IP addresses in '" + AgentsFile.NAME + "' file to the load balancer.")
            .withRequiredArg().ofType(String.class);

    private AwsProvisionerCli() {
    }

    static AwsProvisioner init(String[] args) {
        AwsProvisioner.logHeader();

        AwsProvisionerCli cli = new AwsProvisionerCli();
        initOptionsWithHelp(cli.parser, args);

        SimulatorProperties properties = loadSimulatorProperties();
        ComponentRegistry componentRegistry = loadComponentRegister(new File(getUserDir(), AgentsFile.NAME), false);

        try {
            String awsCredentialsPath = properties.get("AWS_CREDENTIALS", "awscredentials.properties");
            File credentialsFile = new File(awsCredentialsPath);
            AWSCredentials credentials = new PropertiesCredentials(credentialsFile);
            AmazonEC2 ec2 = new AmazonEC2Client(credentials);
            AmazonElasticLoadBalancingClient elb = new AmazonElasticLoadBalancingClient(credentials);

            return new AwsProvisioner(ec2, elb, componentRegistry, properties);
        } catch (Exception e) {
            throw new CommandLineExitException("Credentials file could not be loaded", e);
        }
    }

    static void run(String[] args, AwsProvisioner provisioner) {
        AwsProvisionerCli cli = new AwsProvisionerCli();
        OptionSet options = initOptionsWithHelp(cli.parser, args);

        try {
            if (options.has(cli.scaleSpec)) {
                int count = options.valueOf(cli.scaleSpec);
                provisioner.scaleInstanceCountTo(count);
            } else if (options.has(cli.createLoadBalancerSpec)) {
                String name = options.valueOf(cli.createLoadBalancerSpec);
                provisioner.createLoadBalancer(name);
            } else if (options.has(cli.addAgentsToLoadBalancer)) {
                String name = options.valueOf(cli.addAgentsToLoadBalancer);
                provisioner.addAgentsToLoadBalancer(name);
            } else {
                printHelpAndExit(cli.parser);
            }
        } finally {
            provisioner.shutdown();
        }
    }
}
