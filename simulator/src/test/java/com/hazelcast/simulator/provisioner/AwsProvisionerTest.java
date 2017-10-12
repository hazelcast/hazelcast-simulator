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

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.model.DescribeInstancesRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.Reservation;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.services.ec2.model.RunInstancesResult;
import com.amazonaws.services.elasticloadbalancing.AmazonElasticLoadBalancingClient;
import com.amazonaws.services.elasticloadbalancing.model.CreateLoadBalancerRequest;
import com.amazonaws.services.elasticloadbalancing.model.CreateLoadBalancerResult;
import com.amazonaws.services.elasticloadbalancing.model.DescribeLoadBalancersRequest;
import com.amazonaws.services.elasticloadbalancing.model.DescribeLoadBalancersResult;
import com.amazonaws.services.elasticloadbalancing.model.LoadBalancerDescription;
import com.amazonaws.services.elasticloadbalancing.model.RegisterInstancesWithLoadBalancerRequest;
import com.hazelcast.simulator.common.SimulatorProperties;
import com.hazelcast.simulator.protocol.registry.ComponentRegistry;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

import static com.hazelcast.simulator.TestEnvironmentUtils.createAgentsFileWithLocalhost;
import static com.hazelcast.simulator.TestEnvironmentUtils.deleteAgentsFile;
import static com.hazelcast.simulator.TestEnvironmentUtils.setupFakeEnvironment;
import static com.hazelcast.simulator.TestEnvironmentUtils.tearDownFakeEnvironment;
import static com.hazelcast.simulator.utils.FileUtils.deleteQuiet;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class AwsProvisionerTest {

    private AmazonEC2 ec2;
    private AmazonElasticLoadBalancingClient elb;

    private SimulatorProperties properties;
    private ComponentRegistry componentRegistry;

    private AwsProvisioner provisioner;

    @Before
    public void before() {
        setupFakeEnvironment();
        createAgentsFileWithLocalhost();

        ec2 = mock(AmazonEC2.class);
        elb = mock(AmazonElasticLoadBalancingClient.class);

        properties = new SimulatorProperties();
        properties.set("ELB_ZONES", "a,b,c");

        componentRegistry = new ComponentRegistry();
        componentRegistry.addAgent("127.0.0.1", "127.0.0.1");

        provisioner = new AwsProvisioner(ec2, elb, componentRegistry, properties, 3, 100);
    }

    @After
    public void after() {
        provisioner.shutdown();

        tearDownFakeEnvironment();
        deleteAgentsFile();

        deleteQuiet(AwsProvisioner.AWS_ELB_FILE_NAME);
    }

    @Test
    public void testScaleInstanceCountTo_toOne() {
        mockAmazonEC2ForGetInstancesByPublicIp();

        provisioner.scaleInstanceCountTo(1);
        assertEquals(1, componentRegistry.agentCount());
    }

    @Test
    public void testScaleInstanceCountTo_toZero() {
        mockAmazonEC2ForGetInstancesByPublicIp();

        provisioner.scaleInstanceCountTo(0);
        assertEquals(0, componentRegistry.agentCount());
    }

    @Test
    public void testScaleInstanceCountTo_toTwo() {
        mockAmazonEC2ForCreateInstances();

        provisioner.scaleInstanceCountTo(2);
        assertEquals(2, componentRegistry.agentCount());
    }

    @Test
    public void testScaleInstanceCountTo_toTwo_withEmptySubnetId_withTimeout() {
        properties.set("SUBNET_ID", "");
        provisioner = new AwsProvisioner(ec2, elb, componentRegistry, properties, 1, 100);
        mockAmazonEC2ForCreateInstances();

        provisioner.scaleInstanceCountTo(2);
        assertEquals(1, componentRegistry.agentCount());
    }

    @Test
    public void testCreateLoadBalancer() {
        mockAmazonElasticLoadBalancingClientForCreateLoadBalancer();

        provisioner.createLoadBalancer("awsLoadBalancer");

        verify(elb).createLoadBalancer(any(CreateLoadBalancerRequest.class));
    }

    @Test
    public void testAddAgentsToLoadBalancer() {
        mockAmazonElasticLoadBalancingClientForAddAgentsToLoadBalancer(false, false);
        mockAmazonEC2ForGetInstancesByPublicIp();

        provisioner.addAgentsToLoadBalancer("existingLoadBalancer");

        verify(elb).registerInstancesWithLoadBalancer(any(RegisterInstancesWithLoadBalancerRequest.class));
    }

    @Test
    public void testAddAgentsToLoadBalancer_noInstances() {
        mockAmazonElasticLoadBalancingClientForAddAgentsToLoadBalancer(false, false);
        mockAmazonElasticLoadBalancingClientForCreateLoadBalancer();
        mockAmazonEC2ForGetInstancesByPublicIpWithEmptyList();

        provisioner.addAgentsToLoadBalancer("existingLoadBalancer");

        verify(elb, never()).registerInstancesWithLoadBalancer(any(RegisterInstancesWithLoadBalancerRequest.class));
    }

    @Test
    public void testAddAgentsToLoadBalancer_balancerIsNotAlive() {
        mockAmazonElasticLoadBalancingClientForAddAgentsToLoadBalancer(true, false);
        mockAmazonElasticLoadBalancingClientForCreateLoadBalancer();
        mockAmazonEC2ForGetInstancesByPublicIp();

        provisioner.addAgentsToLoadBalancer("existingLoadBalancer");

        verify(elb).createLoadBalancer(any(CreateLoadBalancerRequest.class));
        verify(elb).registerInstancesWithLoadBalancer(any(RegisterInstancesWithLoadBalancerRequest.class));
    }

    @Test
    public void testAddAgentsToLoadBalancer_balancerIsNotAlive_withException() {
        mockAmazonElasticLoadBalancingClientForAddAgentsToLoadBalancer(true, true);
        mockAmazonElasticLoadBalancingClientForCreateLoadBalancer();
        mockAmazonEC2ForGetInstancesByPublicIp();

        provisioner.addAgentsToLoadBalancer("existingLoadBalancer");

        verify(elb).createLoadBalancer(any(CreateLoadBalancerRequest.class));
        verify(elb).registerInstancesWithLoadBalancer(any(RegisterInstancesWithLoadBalancerRequest.class));
    }

    private void mockAmazonEC2ForGetInstancesByPublicIp() {
        Instance instance = mock(Instance.class);
        when(instance.getInstanceId()).thenReturn("instanceId");

        DescribeInstancesResult describeInstancesResult = mockDescribeInstancesResultWithInstance(instance);

        when(ec2.describeInstances(any(DescribeInstancesRequest.class))).thenReturn(describeInstancesResult);
    }

    private void mockAmazonEC2ForCreateInstances() {
        Instance instance = mock(Instance.class, RETURNS_DEEP_STUBS);
        when(instance.getInstanceId()).thenReturn("instanceId");
        when(instance.getPrivateIpAddress()).thenReturn("192.168.0.1");
        when(instance.getPublicIpAddress()).thenReturn(null).thenReturn("172.16.16.1");
        when(instance.getState().getName()).thenReturn("waiting").thenReturn("running");

        RunInstancesResult runInstancesResult = mock(RunInstancesResult.class, RETURNS_DEEP_STUBS);
        when(runInstancesResult.getReservation().getInstances()).thenReturn(singletonList(instance));

        DescribeInstancesResult describeInstancesResult = mockDescribeInstancesResultWithInstance(instance);

        when(ec2.runInstances(any(RunInstancesRequest.class))).thenReturn(runInstancesResult);
        when(ec2.describeInstances(any(DescribeInstancesRequest.class))).thenReturn(describeInstancesResult);
    }

    private void mockAmazonEC2ForGetInstancesByPublicIpWithEmptyList() {
        DescribeInstancesResult describeInstancesResult = mock(DescribeInstancesResult.class);
        when(describeInstancesResult.getReservations()).thenReturn(Collections.<Reservation>emptyList());

        when(ec2.describeInstances(any(DescribeInstancesRequest.class))).thenReturn(describeInstancesResult);
    }

    private void mockAmazonElasticLoadBalancingClientForCreateLoadBalancer() {
        CreateLoadBalancerResult lbResult = mock(CreateLoadBalancerResult.class);
        when(lbResult.getDNSName()).thenReturn("172.16.16.1");

        when(elb.createLoadBalancer(any(CreateLoadBalancerRequest.class))).thenReturn(lbResult);
    }

    private void mockAmazonElasticLoadBalancingClientForAddAgentsToLoadBalancer(boolean isDescriptionEmpty,
                                                                                boolean throwException) {
        List<LoadBalancerDescription> emptyList = emptyList();
        LoadBalancerDescription description = mock(LoadBalancerDescription.class);

        DescribeLoadBalancersResult firstResult = mock(DescribeLoadBalancersResult.class);
        when(firstResult.getLoadBalancerDescriptions()).thenReturn(isDescriptionEmpty ? emptyList : singletonList(description));

        if (throwException) {
            AmazonServiceException exception = new AmazonServiceException("expected exception");
            when(elb.describeLoadBalancers(any(DescribeLoadBalancersRequest.class))).thenThrow(exception);
        } else {
            when(elb.describeLoadBalancers(any(DescribeLoadBalancersRequest.class))).thenReturn(firstResult);
        }
    }

    private DescribeInstancesResult mockDescribeInstancesResultWithInstance(Instance instance) {
        Reservation reservation = mock(Reservation.class);
        when(reservation.getInstances()).thenReturn(singletonList(instance));

        DescribeInstancesResult describeInstancesResult = mock(DescribeInstancesResult.class);
        when(describeInstancesResult.getReservations()).thenReturn(singletonList(reservation));
        return describeInstancesResult;
    }
}
