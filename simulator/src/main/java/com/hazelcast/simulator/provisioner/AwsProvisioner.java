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
import com.amazonaws.services.ec2.model.TerminateInstancesResult;
import com.amazonaws.services.elasticloadbalancing.AmazonElasticLoadBalancingClient;
import com.amazonaws.services.elasticloadbalancing.model.CreateLoadBalancerRequest;
import com.amazonaws.services.elasticloadbalancing.model.CreateLoadBalancerResult;
import com.amazonaws.services.elasticloadbalancing.model.DescribeLoadBalancersRequest;
import com.amazonaws.services.elasticloadbalancing.model.DescribeLoadBalancersResult;
import com.amazonaws.services.elasticloadbalancing.model.Listener;
import com.amazonaws.services.elasticloadbalancing.model.LoadBalancerDescription;
import com.amazonaws.services.elasticloadbalancing.model.RegisterInstancesWithLoadBalancerRequest;
import com.amazonaws.services.elasticloadbalancing.model.RegisterInstancesWithLoadBalancerResult;
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
import static java.lang.String.format;


/*
* An aws specific provisioning class which is using the AWS sdk to create aws instances,  and aws elastic load balance
* */
public class AwsProvisioner{

    private final static org.apache.log4j.Logger log = org.apache.log4j.Logger.getLogger(Provisioner.class);

    //AWS specific magic strings
    public static final String AWS_RUNNING_STATE  = "running";
    public static final String AWS_SYSTEM_STATUS_OK = "ok";
    public static final String AWS_PUBLIC_IP_FILTER = "ip-address";
    public static final String AWS_KET_NAME_FILTER = "key-name";

    //the file wich will hole the public domain name of the created load balance
    public static final String AWS_ELB_FILE_NAME = "aws-elb.txt";

    private static final int SLEEPING_MS = 1000 * 30;
    private static final int MAX_SLEEPING_ITTERATIONS = 12;

    private AWSCredentials credentials;
    private AmazonEC2 ec2;
    private AmazonElasticLoadBalancingClient elb;

    private SimulatorProperties props = new SimulatorProperties();

    private final File agentsFile = new File("agents.txt");
    private final File elbFile = new File(AWS_ELB_FILE_NAME);
    private File credentialsFile;

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

    public void setProperties(File file) throws IOException{
        props.init(file);

        String awsCredentialsPath = props.get("AWS_CREDENTIALS", "awscredentials.properties");
        credentialsFile = new File(awsCredentialsPath);

        elbProtocol = props.get("ELB_PROTOCOL");

        elbPortIn = Integer.parseInt(props.get("ELB_PORT_IN", "0"));
        elbPortOut = Integer.parseInt(props.get("ELB_PORT_OUT", "0"));
        elbAvailabilityZones = props.get("ELB_ZONES");

        awsKeyName = props.get("AWS_KEY_NAME");
        awsAmi = props.get("AWS_AMI");
        awsBoxId = props.get("AWS_BOXID");
        securityGroup = props.get("SECURITY_GROUP");
        subNetId = props.get("SUBNET_ID", "");

        credentials = new PropertiesCredentials(credentialsFile);

        ec2 = new AmazonEC2Client(credentials);
        elb = new AmazonElasticLoadBalancingClient(credentials);
    }

    public void addAgentsToLoadBalancer(String elbName){

        if( ! isBalancerAlive(elbName) ){
            createLoadBalancer(elbName);
        }

        List<Instance> instances = getInstancesFromAgentsFile();

        addInstancesToElb(elbName, instances);
    }

    public void scaleInstanceCountTo(int totalInstancesWanted){
        List agents = AgentsFile.load(agentsFile);

        if(totalInstancesWanted <= agents.size()){
            terminateInstances(agents.size() - totalInstancesWanted);
            return;
        }
        int instanceCount = totalInstancesWanted - agents.size();

        createInstances(instanceCount);
    }

    private List<Instance> createInstances(int instanceCount){

        RunInstancesRequest runInstancesRequest = new RunInstancesRequest();

        runInstancesRequest.withImageId(awsAmi)
                .withInstanceType(awsBoxId)
                .withMinCount(instanceCount)
                .withMaxCount(instanceCount)
                .withKeyName(awsKeyName);

        if(subNetId.equals("")){
            runInstancesRequest.withSecurityGroups(securityGroup);
        } else {
            runInstancesRequest.withSubnetId(subNetId);
        }

        RunInstancesResult runInstancesResult = ec2.runInstances(runInstancesRequest);

        List<Instance> checkedInstances = new ArrayList();
        List<Instance> instances = runInstancesResult.getReservation().getInstances();
        for (Instance i : instances) {
            if(waiteForInstanceStatusRunning(i)){
                addInstanceToAgentsFile(i);
                checkedInstances.add(i);
            }else{
                log.warn("Time out Waiting for running status id=" + i.getInstanceId() );
            }
        }
        return checkedInstances;
    }

    public void terminateInstances(int count){
        List<AgentAddress> agents = AgentsFile.load(agentsFile);

        List<AgentAddress> deadList = agents.subList(0, count);
        agents.removeAll(deadList);
        AgentsFile.save(agentsFile, agents);

        List<Instance> deadInstances = getInstancesByPublicIp(deadList);
        terminateInstances(deadInstances);
    }

    private void terminateInstances(List<Instance> instances){

        List<String> ids = new ArrayList<String>();
        for(Instance i : instances){
            ids.add(i.getInstanceId());
        }

        TerminateInstancesRequest terminateInstancesRequest = new TerminateInstancesRequest();
        terminateInstancesRequest.withInstanceIds(ids);
        TerminateInstancesResult result = ec2.terminateInstances(terminateInstancesRequest);
    }

    public List<Instance> getInstancesFromAgentsFile(){
        List<AgentAddress> currentAgents = AgentsFile.load(agentsFile);
        return getInstancesByPublicIp(currentAgents);
    }

    public List<Instance> getInstancesByPublicIp(List<AgentAddress> agents){

        List<String> ips = new ArrayList<String>();
        for(AgentAddress agent : agents){
            ips.add(agent.publicAddress);
        }

        DescribeInstancesRequest request = new DescribeInstancesRequest();

        Filter filter = new Filter(AWS_PUBLIC_IP_FILTER, ips);

        DescribeInstancesResult result = ec2.describeInstances(request.withFilters(filter));
        List<Reservation> reservations = result.getReservations();

        List<Instance> foundInstances = new ArrayList();

        for (Reservation reservation : reservations) {
            List<Instance> instances = reservation.getInstances();
            foundInstances.addAll(instances);
        }
        return foundInstances;
    }

    public boolean waiteForInstanceStatusRunning(Instance i){

        DescribeInstancesRequest describeInstancesRequest = new DescribeInstancesRequest().withInstanceIds(i.getInstanceId());
        int counter=0;
        while ( counter++ < MAX_SLEEPING_ITTERATIONS ) {

            sleepSeconds(SLEEPING_MS);

            DescribeInstancesResult result = ec2.describeInstances(describeInstancesRequest);

            for ( Reservation r : result.getReservations() ) {
                for ( Instance j : r.getInstances() ) {
                    if ( j.getPublicIpAddress() != null  && j.getState().getName().equals(AWS_RUNNING_STATE) ) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    public void addInstanceToAgentsFile(Instance instance){
        DescribeInstancesRequest describeInstancesRequest = new DescribeInstancesRequest().withInstanceIds(instance.getInstanceId());
        DescribeInstancesResult result = ec2.describeInstances(describeInstancesRequest);

        for(Reservation r : result.getReservations() ){
            for(Instance j : r.getInstances() ){
                appendText(j.getPublicIpAddress() + "," + j.getPrivateIpAddress() + "\n", agentsFile);
            }
        }
    }

    public String createLoadBalancer(String name){
        CreateLoadBalancerRequest lbRequest = new CreateLoadBalancerRequest();
        lbRequest.setLoadBalancerName(name);
        lbRequest.withAvailabilityZones(elbAvailabilityZones.split(","));

        List<Listener> listeners = new ArrayList();
        listeners.add(new Listener(elbProtocol, elbPortIn, elbPortOut));
        lbRequest.setListeners(listeners);

        CreateLoadBalancerResult lbResult=elb.createLoadBalancer(lbRequest);
        lbResult.getDNSName();
        appendText(lbResult.getDNSName() + "\n", elbFile);

        return lbRequest.getLoadBalancerName();
    }

    public boolean isBalancerAlive(String name){
        Collection<String> names = new HashSet();
        names.add(name);

        DescribeLoadBalancersRequest describe = new DescribeLoadBalancersRequest();
        describe.setLoadBalancerNames(names);

        try{
            DescribeLoadBalancersResult res=  elb.describeLoadBalancers(describe);
            List<LoadBalancerDescription> description = res.getLoadBalancerDescriptions();

            if(description.isEmpty()){
                return false;
            }
            return true;

        }catch (AmazonServiceException e) {
            log.fatal(e);
        }
        return false;
    }

    public void addInstancesToElb(String name, List<Instance> instances){
        if(instances.isEmpty()){
            log.info("No instances to add to load balance "+name);
            return;
        }

        //get instance id's
        Collection<com.amazonaws.services.elasticloadbalancing.model.Instance> ids = new ArrayList();
        for (Instance i: instances) {
            ids.add(new com.amazonaws.services.elasticloadbalancing.model.Instance(i.getInstanceId()));
        }

        //register the instances to the balancer
        RegisterInstancesWithLoadBalancerRequest register = new RegisterInstancesWithLoadBalancerRequest();
        register.setLoadBalancerName(name);
        register.setInstances(ids);
        RegisterInstancesWithLoadBalancerResult registerWithLoadBalancerResult = elb.registerInstancesWithLoadBalancer(register);
    }

    public static void main(String[] args) {
        log.info("AWS specfic Provisioner");
        try {
            AwsProvisioner provisioner = new AwsProvisioner();
            AwsProvisionerCli cli = new AwsProvisionerCli(provisioner);
            cli.run(args);

        } catch (Throwable e) {
            log.fatal(e);
            System.exit(1);
        }
    }
}
