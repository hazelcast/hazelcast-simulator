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
import com.hazelcast.simulator.utils.CloudProviderUtils;
import org.jclouds.aws.ec2.compute.AWSEC2TemplateOptions;
import org.jclouds.compute.domain.Template;
import org.jclouds.compute.options.TemplateOptions;
import org.jclouds.ec2.domain.SecurityGroup;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static com.hazelcast.simulator.TestEnvironmentUtils.setupFakeEnvironment;
import static com.hazelcast.simulator.TestEnvironmentUtils.tearDownFakeEnvironment;
import static com.hazelcast.simulator.provisioner.TemplateBuilder.CIDR_RANGE;
import static com.hazelcast.simulator.provisioner.TemplateBuilder.SSH_PORT;
import static java.util.Arrays.asList;
import static org.apache.commons.lang3.ArrayUtils.toObject;
import static org.jclouds.net.domain.IpProtocol.TCP;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class TemplateBuilderTest extends AbstractComputeServiceTest {

    private SimulatorProperties simulatorProperties;
    private int agentPort;
    private int hazelcastStartPort;
    private int hazelcastEndPort;

    @Before
    public void before() {
        setupFakeEnvironment();

        simulatorProperties = new SimulatorProperties();
        simulatorProperties.setCloudProvider(CloudProviderUtils.PROVIDER_EC2);
        simulatorProperties.set("SECURITY_GROUP", SECURITY_GROUP);
        simulatorProperties.set("AGENT_PORT", "1234");

        agentPort = simulatorProperties.getAgentPort();
        hazelcastStartPort = simulatorProperties.getHazelcastPort();
        hazelcastEndPort = hazelcastStartPort + simulatorProperties.getHazelcastPortRangeSize();

        initComputeServiceMock();
    }

    @After
    public void after() {
        tearDownFakeEnvironment();
    }

    @Test
    public void testBuild() {
        Template template = new TemplateBuilder(computeService, simulatorProperties).build();
        TemplateOptions templateOptions = template.getOptions();

        assertInboundPorts(templateOptions);
        assertSecurityGroupApi();
    }

    @Test
    public void testBuild_withPredefinedSecurityGroups() {
        securityGroups.add(mock(SecurityGroup.class));

        Template template = new TemplateBuilder(computeService, simulatorProperties).build();
        TemplateOptions templateOptions = template.getOptions();

        assertInboundPorts(templateOptions);
        verify(securityGroupApi).describeSecurityGroupsInRegion(anyString(), eq(SECURITY_GROUP));
        verifyNoMoreInteractions(securityGroupApi);
    }

    @Test
    public void testBuild_cloudProviderNotEC2() {
        simulatorProperties.setCloudProvider(CloudProviderUtils.PROVIDER_STATIC);

        Template template = new TemplateBuilder(computeService, simulatorProperties).build();
        TemplateOptions templateOptions = template.getOptions();

        assertInboundPorts(templateOptions);
        verifyNoMoreInteractions(securityGroupApi);
    }

    @Test
    public void testBuild_customSubnetId() {
        simulatorProperties.set("SUBNET_ID", "custom");

        Template template = new TemplateBuilder(computeService, simulatorProperties).build();
        TemplateOptions templateOptions = template.getOptions();

        assertEquals("custom", templateOptions.as(AWSEC2TemplateOptions.class).getSubnetId());

        assertInboundPorts(templateOptions);
        verifyNoMoreInteractions(securityGroupApi);
    }

    @Test(expected = IllegalStateException.class)
    public void testBuild_customSubnetId_cloudProviderNotEC2() {
        simulatorProperties.set("SUBNET_ID", "custom");
        simulatorProperties.setCloudProvider(CloudProviderUtils.PROVIDER_STATIC);

        new TemplateBuilder(computeService, simulatorProperties).build();
    }

    private void assertInboundPorts(TemplateOptions templateOptions) {
        int[] inboundPorts = templateOptions.getInboundPorts();
        List<Integer> ports = asList(toObject(inboundPorts));
        assertTrue(ports.contains(SSH_PORT));
        assertTrue(ports.contains(agentPort));
        for (int port = hazelcastStartPort; port < hazelcastEndPort; port++) {
            assertTrue(ports.contains(port));
        }
    }

    private void assertSecurityGroupApi() {
        verify(securityGroupApi).describeSecurityGroupsInRegion(anyString(), eq(SECURITY_GROUP));
        verify(securityGroupApi).createSecurityGroupInRegion(anyString(), eq(SECURITY_GROUP), eq(SECURITY_GROUP));
        verify(securityGroupApi).authorizeSecurityGroupIngressInRegion(anyString(), eq(SECURITY_GROUP), eq(TCP),
                eq(SSH_PORT), eq(SSH_PORT), eq(CIDR_RANGE));
        verify(securityGroupApi).authorizeSecurityGroupIngressInRegion(anyString(), eq(SECURITY_GROUP), eq(TCP),
                eq(agentPort), eq(agentPort), eq(CIDR_RANGE));
        verify(securityGroupApi).authorizeSecurityGroupIngressInRegion(anyString(), eq(SECURITY_GROUP), eq(TCP),
                eq(hazelcastStartPort), eq(hazelcastEndPort), eq(CIDR_RANGE));
        verifyNoMoreInteractions(securityGroupApi);
    }
}
