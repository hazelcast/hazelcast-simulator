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

import com.hazelcast.simulator.common.SimulatorProperties;
import org.apache.log4j.Logger;
import org.jclouds.aws.ec2.AWSEC2Api;
import org.jclouds.aws.ec2.compute.AWSEC2TemplateOptions;
import org.jclouds.compute.ComputeService;
import org.jclouds.compute.domain.Template;
import org.jclouds.compute.domain.TemplateBuilderSpec;
import org.jclouds.ec2.domain.SecurityGroup;
import org.jclouds.ec2.features.SecurityGroupApi;
import org.jclouds.scriptbuilder.statements.login.AdminAccess;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.hazelcast.simulator.utils.CloudProviderUtils.isEC2;
import static org.apache.commons.lang3.ArrayUtils.toPrimitive;
import static org.jclouds.net.domain.IpProtocol.TCP;

final class TemplateBuilder {

    static final int SSH_PORT = 22;
    static final String CIDR_RANGE = "0.0.0.0/0";

    private static final Logger LOGGER = Logger.getLogger(Provisioner.class);

    private final Map<Integer, Integer> portRangeMap = new HashMap<Integer, Integer>();

    private final ComputeService compute;
    private final SimulatorProperties simulatorProperties;

    TemplateBuilder(ComputeService compute, SimulatorProperties simulatorProperties) {
        this.compute = compute;
        this.simulatorProperties = simulatorProperties;

        populatePortRangeMap();
    }

    private void populatePortRangeMap() {
        int agentPort = simulatorProperties.getAgentPort();
        int hazelcastStartPort = simulatorProperties.getHazelcastPort();
        int hazelcastEndPort = hazelcastStartPort + simulatorProperties.getHazelcastPortRangeSize();

        portRangeMap.put(SSH_PORT, SSH_PORT);
        portRangeMap.put(agentPort, agentPort);
        portRangeMap.put(hazelcastStartPort, hazelcastEndPort);
    }

    Template build() {
        LOGGER.info("Creating jclouds template...");

        String securityGroup = simulatorProperties.get("SECURITY_GROUP", "simulator");
        LOGGER.info("Security group: " + securityGroup);

        String machineSpec = simulatorProperties.get("MACHINE_SPEC", "");
        TemplateBuilderSpec spec = TemplateBuilderSpec.parse(machineSpec);
        LOGGER.info("Machine spec: " + machineSpec);

        Template template = buildTemplate(spec);

        String user = simulatorProperties.getUser();
        AdminAccess adminAccess = AdminAccess.builder().adminUsername(user).build();
        LOGGER.info("Login name to remote machines: " + user);

        template.getOptions()
                .inboundPorts(inboundPorts())
                .runScript(adminAccess);

        String subnetId = simulatorProperties.get("SUBNET_ID", "default");
        if (subnetId.equals("default") || subnetId.isEmpty()) {
            initSecurityGroup(spec, securityGroup);
            template.getOptions()
                    .securityGroups(securityGroup);
        } else {
            if (!isEC2(simulatorProperties)) {
                throw new IllegalStateException("SUBNET_ID can be used only when EC2 is configured as a cloud provider.");
            }
            LOGGER.info("Using VPC with Subnet ID = " + subnetId);
            template.getOptions()
                    .as(AWSEC2TemplateOptions.class)
                    .subnetId(subnetId);
        }

        LOGGER.info("Successfully created jclouds template");
        return template;
    }

    private Template buildTemplate(TemplateBuilderSpec spec) {
        return compute.templateBuilder().from(spec).build();
    }

    private int[] inboundPorts() {
        List<Integer> ports = new ArrayList<Integer>();
        for (Map.Entry<Integer, Integer> portRangeEntry : portRangeMap.entrySet()) {
            int startPort = portRangeEntry.getKey();
            int endPort = portRangeEntry.getValue();
            if (startPort == endPort) {
                ports.add(startPort);
                continue;
            }
            for (int port = startPort; port < endPort; port++) {
                ports.add(port);
            }
        }
        return toPrimitive(ports.toArray(new Integer[ports.size()]));
    }

    private void initSecurityGroup(TemplateBuilderSpec spec, String securityGroup) {
        if (!isEC2(simulatorProperties)) {
            return;
        }

        // in case of AWS, we are going to create the security group, if it doesn't exist
        AWSEC2Api ec2Api = compute.getContext().unwrapApi(AWSEC2Api.class);
        SecurityGroupApi securityGroupApi = ec2Api.getSecurityGroupApi().get();
        String region = spec.getLocationId();
        if (region == null) {
            region = "us-east-1";
        }

        Set<SecurityGroup> securityGroups = securityGroupApi.describeSecurityGroupsInRegion(region, securityGroup);
        if (!securityGroups.isEmpty()) {
            LOGGER.info("Security group: '" + securityGroup + "' is found in region '" + region + '\'');
            return;
        }

        LOGGER.info("Security group: '" + securityGroup + "' is not found in region '" + region + "', creating it on the fly");
        securityGroupApi.createSecurityGroupInRegion(region, securityGroup, securityGroup);
        for (Map.Entry<Integer, Integer> portRangeEntry : portRangeMap.entrySet()) {
            int startPort = portRangeEntry.getKey();
            int endPort = portRangeEntry.getValue();
            securityGroupApi.authorizeSecurityGroupIngressInRegion(region, securityGroup, TCP, startPort, endPort, CIDR_RANGE);
        }
    }
}
