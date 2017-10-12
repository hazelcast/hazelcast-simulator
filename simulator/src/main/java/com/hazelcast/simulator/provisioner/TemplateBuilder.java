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

import com.hazelcast.simulator.common.SimulatorProperties;
import org.apache.log4j.Logger;
import org.jclouds.aws.ec2.AWSEC2Api;
import org.jclouds.aws.ec2.compute.AWSEC2TemplateOptions;
import org.jclouds.compute.ComputeService;
import org.jclouds.compute.domain.Template;
import org.jclouds.compute.domain.TemplateBuilderSpec;
import org.jclouds.compute.domain.Volume;
import org.jclouds.compute.options.TemplateOptions;
import org.jclouds.ec2.compute.options.EC2TemplateOptions;
import org.jclouds.ec2.domain.SecurityGroup;
import org.jclouds.ec2.features.SecurityGroupApi;
import org.jclouds.scriptbuilder.ScriptBuilder;
import org.jclouds.scriptbuilder.statements.login.AdminAccess;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.hazelcast.simulator.utils.CloudProviderUtils.isEC2;
import static java.lang.Math.round;
import static java.lang.String.format;
import static org.apache.commons.lang3.ArrayUtils.toPrimitive;
import static org.jclouds.compute.domain.Volume.Type.LOCAL;
import static org.jclouds.net.domain.IpProtocol.TCP;
import static org.jclouds.scriptbuilder.domain.Statements.exec;

final class TemplateBuilder {

    static final int SSH_PORT = 22;
    static final String CIDR_RANGE = "0.0.0.0/0";

    private static final String DEFAULT_SUBNET_ID = "default";
    private static final String DEFAULT_MKFS_OPTIONS = "-t ext4";
    private static final String DEFAULT_MOUNT_OPTIONS = "defaults,nofail,noatime,relatime";

    private static final Logger LOGGER = Logger.getLogger(Provisioner.class);

    private final Map<Integer, Integer> portRangeMap = new HashMap<Integer, Integer>();
    private final ScriptBuilder scriptBuilder = new ScriptBuilder();

    private final ComputeService compute;
    private final SimulatorProperties simulatorProperties;
    private final boolean isEC2;

    TemplateBuilder(ComputeService compute, SimulatorProperties simulatorProperties) {
        this.compute = compute;
        this.simulatorProperties = simulatorProperties;
        this.isEC2 = isEC2(simulatorProperties);

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

        String user = simulatorProperties.getUser();
        LOGGER.info("Login name to remote machines: " + user);

        String securityGroup = simulatorProperties.get("SECURITY_GROUP", "simulator");
        LOGGER.info("Security group: " + securityGroup);

        String machineSpec = simulatorProperties.get("MACHINE_SPEC", "");
        TemplateBuilderSpec spec = TemplateBuilderSpec.parse(machineSpec);
        LOGGER.info("Machine spec: " + machineSpec);

        Template template = compute.templateBuilder().from(spec).build();
        TemplateOptions templateOptions = template.getOptions();

        Float spotPrice = simulatorProperties.getAsFloat("EC2_SPOT_PRICE");
        if (spotPrice != null) {
            LOGGER.info("EC2_SPOT_PRICE: " + spotPrice);
            templateOptions.as(AWSEC2TemplateOptions.class).spotPrice(spotPrice);
        }

        addAdminAccess(user);

        templateOptions.inboundPorts(inboundPorts());

        String subnetId = simulatorProperties.get("SUBNET_ID", DEFAULT_SUBNET_ID);
        if (DEFAULT_SUBNET_ID.equals(subnetId) || subnetId.isEmpty()) {
            initSecurityGroup(spec, securityGroup);
            templateOptions.securityGroups(securityGroup);
        } else {
            if (!isEC2) {
                throw new IllegalStateException("SUBNET_ID can be used only when EC2 is configured as a cloud provider.");
            }
            LOGGER.info("Using VPC with Subnet ID = " + subnetId);
            templateOptions
                    .as(AWSEC2TemplateOptions.class)
                    .subnetId(subnetId);
        }

        if (isEC2) {
            EC2TemplateOptions ec2TemplateOptions = templateOptions.as(EC2TemplateOptions.class);
            mapDevices(ec2TemplateOptions, template, user);
        }

        templateOptions.runScript(scriptBuilder);

        LOGGER.info("Successfully created jclouds template");
        return template;
    }

    private void addAdminAccess(String user) {
        AdminAccess adminAccess = AdminAccess.builder().adminUsername(user).build();
        scriptBuilder.addStatement(adminAccess);
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
        if (!isEC2) {
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

    private void mapDevices(EC2TemplateOptions ec2TemplateOptions, Template template, String user) {
        String mkfsOptions = simulatorProperties.get("INSTANCE_STORAGE_MKFS_OPTIONS", DEFAULT_MKFS_OPTIONS);
        String mountOptions = simulatorProperties.get("INSTANCE_STORAGE_MOUNT_OPTIONS", DEFAULT_MOUNT_OPTIONS);

        int ephemeralCounter = 0;
        for (Volume volume : template.getHardware().getVolumes()) {
            if (!volume.isBootDevice() && LOCAL.equals(volume.getType())) {
                String device = volume.getDevice();

                LOGGER.info(format("Mapping device %s (%d GB)", device, round(volume.getSize())));
                ec2TemplateOptions.mapEphemeralDeviceToDeviceName(device, "ephemeral" + ephemeralCounter++);

                mountDevice(device, mkfsOptions, mountOptions, user);
            }
        }
    }

    private void mountDevice(String device, String mkfsOptions, String mountOptions, String user) {
        String mountName = device.substring(device.lastIndexOf('/') + 1);

        if (device.startsWith("/dev/sd")) {
            String virtualDevice = device.replace("/dev/sd", "/dev/xvd");
            addStatement("ln -s %s %s || true", virtualDevice, device);
        }
        addStatement("mkfs %s %s", mkfsOptions, device);

        addStatement("mkdir /mnt/%s", mountName);
        addStatement("mount -o %s %s /mnt/%s", mountOptions, device, mountName);

        addStatement("chown -R %s /mnt/%s", user, mountName);
        addStatement("ln -s /mnt/%s /home/users/%s/%s", mountName, user, mountName);
    }

    private void addStatement(String command, Object... options) {
        String statement = format(command, options);
        scriptBuilder.addStatement(exec(statement));
    }
}
