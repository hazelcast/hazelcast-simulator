package com.hazelcast.simulator.provisioner;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;
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
import com.hazelcast.simulator.common.AgentAddress;
import com.hazelcast.simulator.common.AgentsFile;
import com.hazelcast.simulator.common.SimulatorProperties;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

import static com.hazelcast.simulator.utils.CommonUtils.sleepSeconds;
import static com.hazelcast.simulator.utils.FileUtils.appendText;

/**
 * An AWS specific provisioning class which is using the AWS SDK to create AWS instances and AWS elastic load balancer.
 */
public class AwsProvisioner {


    // AWS specific magic strings
    public static final String AWS_RUNNING_STATE = "running";
    public static final String AWS_SYSTEM_STATUS_OK = "ok";
    public static final String AWS_PUBLIC_IP_FILTER = "ip-address";
    public static final String AWS_KET_NAME_FILTER = "key-name";

    // the file which will hole the public domain name of the created load balance
    public static final String AWS_ELB_FILE_NAME = "aws-elb.txt";

    private static final int SLEEPING_MS = 1000 * 30;
    private static final int MAX_SLEEPING_ITERATIONS = 12;
    private static final org.apache.log4j.Logger LOGGER = org.apache.log4j.Logger.getLogger(Provisioner.class);

    private AmazonEC2 ec2;
    private AmazonElasticLoadBalancingClient elb;
    private SimulatorProperties props = new SimulatorProperties();
    private final File agentsFile = new File(Provisioner.AGENTS_FILE);
    private final File elbFile = new File(AWS_ELB_FILE_NAME);

    private String securityGroup;
    private String awsKeyName;
    private String awsAmi;
    private String awsBoxId;
    private String subNetId;

    private String elbProtocol;
    private int elbPortIn;
    private int elbPortOut;
    private String elbAvailabilityZones;

    public AwsProvisioner() throws IOException {
        setProperties(null);
    }

    public void setProperties(File file) throws IOException {
        props.init(file);

        String awsCredentialsPath = props.get("AWS_CREDENTIALS", "awscredentials.properties");
        File credentialsFile = new File(awsCredentialsPath);

        elbProtocol = props.get("ELB_PROTOCOL");

        elbPortIn = Integer.parseInt(props.get("ELB_PORT_IN", "0"));
        elbPortOut = Integer.parseInt(props.get("ELB_PORT_OUT", "0"));
        elbAvailabilityZones = props.get("ELB_ZONES");

        awsKeyName = props.get("AWS_KEY_NAME");
        awsAmi = props.get("AWS_AMI");
        awsBoxId = props.get("AWS_BOXID");
        securityGroup = props.get("SECURITY_GROUP");
        subNetId = props.get("SUBNET_ID", "");

        AWSCredentials credentials = new PropertiesCredentials(credentialsFile);

        ec2 = new AmazonEC2Client(credentials);
        elb = new AmazonElasticLoadBalancingClient(credentials);
    }

    public String createLoadBalancer(String name) {
        CreateLoadBalancerRequest lbRequest = new CreateLoadBalancerRequest();
        lbRequest.setLoadBalancerName(name);
        lbRequest.withAvailabilityZones(elbAvailabilityZones.split(","));

        List<Listener> listeners = new ArrayList<Listener>();
        listeners.add(new Listener(elbProtocol, elbPortIn, elbPortOut));
        lbRequest.setListeners(listeners);

        CreateLoadBalancerResult lbResult = elb.createLoadBalancer(lbRequest);
        lbResult.getDNSName();
        appendText(lbResult.getDNSName() + "\n", elbFile);

        return lbRequest.getLoadBalancerName();
    }

    public void addAgentsToLoadBalancer(String elbName) {
        if (!isBalancerAlive(elbName)) {
            createLoadBalancer(elbName);
        }

        List<Instance> instances = getInstancesFromAgentsFile();

        addInstancesToElb(elbName, instances);
    }

    public void scaleInstanceCountTo(int totalInstancesWanted) {
        List agents = AgentsFile.load(agentsFile);

        if (totalInstancesWanted <= agents.size()) {
            terminateInstances(agents.size() - totalInstancesWanted);
            return;
        }
        int instanceCount = totalInstancesWanted - agents.size();

        createInstances(instanceCount);
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
            if (waiteForInstanceStatusRunning(instance)) {
                addInstanceToAgentsFile(instance);
                checkedInstances.add(instance);
            } else {
                LOGGER.warn("Timeout waiting for running status id=" + instance.getInstanceId());
            }
        }
        return checkedInstances;
    }

    private void terminateInstances(int count) {
        List<AgentAddress> agents = AgentsFile.load(agentsFile);

        List<AgentAddress> deadList = agents.subList(0, count);
        agents.removeAll(deadList);
        AgentsFile.save(agentsFile, agents);

        List<Instance> deadInstances = getInstancesByPublicIp(deadList);
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

    private List<Instance> getInstancesFromAgentsFile() {
        List<AgentAddress> currentAgents = AgentsFile.load(agentsFile);
        return getInstancesByPublicIp(currentAgents);
    }

    private List<Instance> getInstancesByPublicIp(List<AgentAddress> agents) {
        List<String> ips = new ArrayList<String>();
        for (AgentAddress agent : agents) {
            ips.add(agent.publicAddress);
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

    private boolean waiteForInstanceStatusRunning(Instance instance) {
        String instanceId = instance.getInstanceId();
        DescribeInstancesRequest describeInstancesRequest = new DescribeInstancesRequest().withInstanceIds(instanceId);
        int counter = 0;
        while (counter++ < MAX_SLEEPING_ITERATIONS) {
            sleepSeconds(SLEEPING_MS);

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
                appendText(reserved.getPublicIpAddress() + "," + reserved.getPrivateIpAddress() + "\n", agentsFile);
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
            LOGGER.fatal(e);
        }
        return false;
    }

    private void addInstancesToElb(String name, List<Instance> instances) {
        if (instances.isEmpty()) {
            LOGGER.info("No instances to add to load balance " + name);
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
        LOGGER.info("AWS specific provisioner");
        try {
            AwsProvisioner provisioner = new AwsProvisioner();
            AwsProvisionerCli cli = new AwsProvisionerCli(provisioner);
            cli.run(args);

        } catch (Throwable e) {
            LOGGER.fatal(e);
            System.exit(1);
        }
    }
}
