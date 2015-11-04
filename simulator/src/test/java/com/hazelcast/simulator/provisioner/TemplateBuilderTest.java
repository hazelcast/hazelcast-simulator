package com.hazelcast.simulator.provisioner;

import com.hazelcast.simulator.common.SimulatorProperties;
import org.jclouds.aws.ec2.compute.AWSEC2TemplateOptions;
import org.jclouds.compute.domain.Template;
import org.jclouds.compute.options.TemplateOptions;
import org.jclouds.ec2.domain.SecurityGroup;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static com.hazelcast.simulator.TestEnvironmentUtils.deleteLogs;
import static com.hazelcast.simulator.TestEnvironmentUtils.resetUserDir;
import static com.hazelcast.simulator.TestEnvironmentUtils.setDistributionUserDir;
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
    public void setUp() {
        setDistributionUserDir();

        simulatorProperties = new SimulatorProperties();
        simulatorProperties.set("SECURITY_GROUP", SECURITY_GROUP);
        simulatorProperties.set("AGENT_PORT", "1234");

        agentPort = simulatorProperties.getAgentPort();
        hazelcastStartPort = simulatorProperties.getHazelcastPort();
        hazelcastEndPort = hazelcastStartPort + simulatorProperties.getHazelcastPortRangeSize();

        initComputeServiceMock();
    }

    @After
    public void tearDown() {
        resetUserDir();
        deleteLogs();
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
        simulatorProperties.set("CLOUD_PROVIDER", "static");

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
        simulatorProperties.set("CLOUD_PROVIDER", "static");

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
