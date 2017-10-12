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

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.model.DescribeInstancesRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.amazonaws.services.ec2.model.Filter;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.Reservation;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.services.ec2.model.RunInstancesResult;
import com.amazonaws.services.ec2.model.TerminateInstancesRequest;
import com.amazonaws.services.elasticloadbalancing.AmazonElasticLoadBalancingClient;
import com.amazonaws.services.elasticloadbalancing.model.CreateLoadBalancerRequest;
import com.amazonaws.services.elasticloadbalancing.model.CreateLoadBalancerResult;
import com.amazonaws.services.elasticloadbalancing.model.DescribeLoadBalancersRequest;
import com.amazonaws.services.elasticloadbalancing.model.DescribeLoadBalancersResult;
import com.amazonaws.services.elasticloadbalancing.model.Listener;
import com.amazonaws.services.elasticloadbalancing.model.LoadBalancerDescription;
import com.amazonaws.services.elasticloadbalancing.model.RegisterInstancesWithLoadBalancerRequest;
import com.hazelcast.simulator.common.AgentsFile;
import com.hazelcast.simulator.common.SimulatorProperties;
import com.hazelcast.simulator.protocol.registry.AgentData;
import com.hazelcast.simulator.protocol.registry.ComponentRegistry;
import org.apache.log4j.Logger;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.simulator.common.GitInfo.getBuildTime;
import static com.hazelcast.simulator.common.GitInfo.getCommitIdAbbrev;
import static com.hazelcast.simulator.provisioner.AwsProvisionerCli.init;
import static com.hazelcast.simulator.provisioner.AwsProvisionerCli.run;
import static com.hazelcast.simulator.utils.CommonUtils.exitWithError;
import static com.hazelcast.simulator.utils.CommonUtils.getSimulatorVersion;
import static com.hazelcast.simulator.utils.CommonUtils.sleepMillis;
import static com.hazelcast.simulator.utils.FileUtils.appendText;
import static com.hazelcast.simulator.utils.FileUtils.getSimulatorHome;
import static com.hazelcast.simulator.utils.FileUtils.getUserDir;
import static com.hazelcast.simulator.utils.FormatUtils.NEW_LINE;
import static java.lang.String.format;

/**
 * An AWS specific provisioning class which is using the AWS SDK to create AWS instances and AWS elastic load balancer.
 */
public class AwsProvisioner {

    // the file which will hold the public domain name of the created load balancer
    static final String AWS_ELB_FILE_NAME = "aws-elb.txt";

    // AWS specific magic strings
    private static final String AWS_RUNNING_STATE = "running";
    private static final String AWS_PUBLIC_IP_FILTER = "ip-address";

    private static final int SLEEPING_MILLIS = (int) TimeUnit.SECONDS.toMillis(30);
    private static final int MAX_SLEEPING_ITERATIONS = 12;

    private static final Logger LOGGER = Logger.getLogger(Provisioner.class);

    private final File agentsFile = new File(getUserDir(), AgentsFile.NAME);
    private final File elbFile = new File(getUserDir(), AWS_ELB_FILE_NAME);

    private final AmazonEC2 ec2;
    private final AmazonElasticLoadBalancingClient elb;
    private final ComponentRegistry componentRegistry;

    private final int maxSleepIterations;
    private final int sleepMillis;

    private final String elbProtocol;
    private final int elbPortIn;
    private final int elbPortOut;
    private final String elbAvailabilityZones;

    private final String securityGroup;
    private final String awsKeyName;
    private final String awsAmi;
    private final String awsBoxId;
    private final String subNetId;

    AwsProvisioner(AmazonEC2 ec2, AmazonElasticLoadBalancingClient elb, ComponentRegistry componentRegistry,
                   SimulatorProperties properties) {
        this(ec2, elb, componentRegistry, properties, MAX_SLEEPING_ITERATIONS, SLEEPING_MILLIS);
    }

    AwsProvisioner(AmazonEC2 ec2, AmazonElasticLoadBalancingClient elb, ComponentRegistry componentRegistry,
                   SimulatorProperties properties, int maxSleepIterations, int sleepMillis) {
        this.ec2 = ec2;
        this.elb = elb;
        this.componentRegistry = componentRegistry;

        this.maxSleepIterations = maxSleepIterations;
        this.sleepMillis = sleepMillis;

        this.elbProtocol = properties.get("ELB_PROTOCOL");
        this.elbPortIn = Integer.parseInt(properties.get("ELB_PORT_IN", "0"));
        this.elbPortOut = Integer.parseInt(properties.get("ELB_PORT_OUT", "0"));
        this.elbAvailabilityZones = properties.get("ELB_ZONES");

        this.securityGroup = properties.get("SECURITY_GROUP");
        this.awsKeyName = properties.get("AWS_KEY_NAME");
        this.awsAmi = properties.get("AWS_AMI");
        this.awsBoxId = properties.get("AWS_BOXID");
        this.subNetId = properties.get("SUBNET_ID", "");
    }

    void scaleInstanceCountTo(int totalInstancesWanted) {
        int agentsSize = componentRegistry.agentCount();
        if (totalInstancesWanted > agentsSize) {
            createInstances(totalInstancesWanted - agentsSize);
        } else {
            terminateInstances(agentsSize - totalInstancesWanted);
        }
    }

    String createLoadBalancer(String name) {
        CreateLoadBalancerRequest request = new CreateLoadBalancerRequest();
        request.setLoadBalancerName(name);
        request.withAvailabilityZones(elbAvailabilityZones.split(","));

        List<Listener> listeners = new ArrayList<Listener>();
        listeners.add(new Listener(elbProtocol, elbPortIn, elbPortOut));
        request.setListeners(listeners);

        CreateLoadBalancerResult lbResult = elb.createLoadBalancer(request);
        appendText(lbResult.getDNSName() + NEW_LINE, elbFile);

        return request.getLoadBalancerName();
    }

    void addAgentsToLoadBalancer(String elbName) {
        if (!isBalancerAlive(elbName)) {
            createLoadBalancer(elbName);
        }

        List<Instance> instances = getInstancesByPublicIp(componentRegistry.getAgents(), false);
        addInstancesToElb(elbName, instances);
    }

    void shutdown() {
        ec2.shutdown();
        elb.shutdown();
    }

    private List<Instance> createInstances(int instanceCount) {
        RunInstancesRequest runInstancesRequest = new RunInstancesRequest();
        runInstancesRequest.withImageId(awsAmi)
                .withInstanceType(awsBoxId)
                .withMinCount(instanceCount)
                .withMaxCount(instanceCount)
                .withKeyName(awsKeyName);

        if (subNetId.isEmpty()) {
            runInstancesRequest.withSecurityGroups(securityGroup);
        } else {
            runInstancesRequest.withSubnetId(subNetId);
        }

        RunInstancesResult runInstancesResult = ec2.runInstances(runInstancesRequest);

        List<Instance> checkedInstances = new ArrayList<Instance>();
        List<Instance> instances = runInstancesResult.getReservation().getInstances();
        for (Instance instance : instances) {
            if (waitForInstanceStatusRunning(instance)) {
                addInstanceToAgentsFile(instance);
                checkedInstances.add(instance);
            } else {
                LOGGER.warn("Timeout waiting for running status id=" + instance.getInstanceId());
            }
        }
        return checkedInstances;
    }

    private void terminateInstances(int count) {
        List<AgentData> deadList = componentRegistry.getAgents(count);
        List<Instance> deadInstances = getInstancesByPublicIp(deadList, true);

        echo("Updating " + agentsFile.getAbsolutePath());
        AgentsFile.save(agentsFile, componentRegistry);

        terminateInstances(deadInstances);
    }

    private void terminateInstances(List<Instance> instances) {
        List<String> ids = new ArrayList<String>();
        for (Instance instance : instances) {
            ids.add(instance.getInstanceId());
        }

        TerminateInstancesRequest terminateInstancesRequest = new TerminateInstancesRequest();
        terminateInstancesRequest.withInstanceIds(ids);
        ec2.terminateInstances(terminateInstancesRequest);
    }

    private List<Instance> getInstancesByPublicIp(List<AgentData> agentDataList, boolean removeFromRegistry) {
        List<String> ips = new ArrayList<String>();
        for (AgentData agentData : agentDataList) {
            ips.add(agentData.getPublicAddress());
            if (removeFromRegistry) {
                componentRegistry.removeAgent(agentData);
            }
        }

        DescribeInstancesRequest request = new DescribeInstancesRequest();
        Filter filter = new Filter(AWS_PUBLIC_IP_FILTER, ips);

        DescribeInstancesResult result = ec2.describeInstances(request.withFilters(filter));
        List<Reservation> reservations = result.getReservations();
        List<Instance> foundInstances = new ArrayList<Instance>();

        for (Reservation reservation : reservations) {
            List<Instance> instances = reservation.getInstances();
            foundInstances.addAll(instances);
        }
        return foundInstances;
    }

    private boolean waitForInstanceStatusRunning(Instance instance) {
        String instanceId = instance.getInstanceId();
        DescribeInstancesRequest describeInstancesRequest = new DescribeInstancesRequest().withInstanceIds(instanceId);
        int counter = 0;
        while (counter++ < maxSleepIterations) {
            sleepMillis(sleepMillis);

            DescribeInstancesResult result = ec2.describeInstances(describeInstancesRequest);
            for (Reservation reservation : result.getReservations()) {
                for (Instance reserved : reservation.getInstances()) {
                    if (reserved.getPublicIpAddress() != null && AWS_RUNNING_STATE.equals(reserved.getState().getName())) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    private void addInstanceToAgentsFile(Instance instance) {
        String instanceId = instance.getInstanceId();
        DescribeInstancesRequest describeInstancesRequest = new DescribeInstancesRequest().withInstanceIds(instanceId);
        DescribeInstancesResult result = ec2.describeInstances(describeInstancesRequest);

        for (Reservation reservation : result.getReservations()) {
            for (Instance reserved : reservation.getInstances()) {
                appendText(reserved.getPublicIpAddress() + ',' + reserved.getPrivateIpAddress() + NEW_LINE, agentsFile);
                componentRegistry.addAgent(reserved.getPublicIpAddress(), reserved.getPrivateIpAddress());
            }
        }
    }

    private boolean isBalancerAlive(String name) {
        Collection<String> names = new HashSet<String>();
        names.add(name);

        DescribeLoadBalancersRequest describe = new DescribeLoadBalancersRequest();
        describe.setLoadBalancerNames(names);

        try {
            DescribeLoadBalancersResult result = elb.describeLoadBalancers(describe);
            List<LoadBalancerDescription> description = result.getLoadBalancerDescriptions();

            if (description.isEmpty()) {
                return false;
            }
            return true;
        } catch (AmazonServiceException e) {
            LOGGER.fatal("Exception in isBalancerAlive(" + name + ')', e);
        }
        return false;
    }

    private void addInstancesToElb(String name, List<Instance> instances) {
        if (instances.isEmpty()) {
            echo("No instances to add to load balance " + name);
            return;
        }

        // get instance ids
        Collection<com.amazonaws.services.elasticloadbalancing.model.Instance> ids
                = new ArrayList<com.amazonaws.services.elasticloadbalancing.model.Instance>();
        for (Instance instance : instances) {
            ids.add(new com.amazonaws.services.elasticloadbalancing.model.Instance(instance.getInstanceId()));
        }

        // register the instances to the balancer
        RegisterInstancesWithLoadBalancerRequest register = new RegisterInstancesWithLoadBalancerRequest();
        register.setLoadBalancerName(name);
        register.setInstances(ids);
        elb.registerInstancesWithLoadBalancer(register);
    }

    public static void main(String[] args) {
        try {
            run(args, init(args));
        } catch (Exception e) {
            exitWithError(LOGGER, "Could not provision machines", e);
        }
    }

    static void logHeader() {
        echo("Hazelcast Simulator AWS Provisioner");
        echo("Version: %s, Commit: %s, Build Time: %s", getSimulatorVersion(), getCommitIdAbbrev(), getBuildTime());
        echo("SIMULATOR_HOME: %s", getSimulatorHome().getAbsolutePath());
    }

    private static void echo(String message, Object... args) {
        LOGGER.info(message == null ? "null" : format(message, args));
    }
}
