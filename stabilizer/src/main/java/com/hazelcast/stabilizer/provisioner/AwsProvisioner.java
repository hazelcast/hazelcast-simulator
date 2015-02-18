package com.hazelcast.stabilizer.provisioner;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.DescribeInstanceStatusRequest;
import com.amazonaws.services.ec2.model.DescribeInstanceStatusResult;
import com.amazonaws.services.ec2.model.DescribeInstancesRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.amazonaws.services.ec2.model.Filter;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.InstanceStatus;
import com.amazonaws.services.ec2.model.Reservation;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.services.ec2.model.RunInstancesResult;
import com.amazonaws.services.elasticloadbalancing.AmazonElasticLoadBalancingClient;
import com.amazonaws.services.elasticloadbalancing.model.CreateLoadBalancerRequest;
import com.amazonaws.services.elasticloadbalancing.model.CreateLoadBalancerResult;
import com.amazonaws.services.elasticloadbalancing.model.DescribeLoadBalancersRequest;
import com.amazonaws.services.elasticloadbalancing.model.DescribeLoadBalancersResult;
import com.amazonaws.services.elasticloadbalancing.model.Listener;
import com.amazonaws.services.elasticloadbalancing.model.LoadBalancerDescription;
import com.amazonaws.services.elasticloadbalancing.model.RegisterInstancesWithLoadBalancerRequest;
import com.amazonaws.services.elasticloadbalancing.model.RegisterInstancesWithLoadBalancerResult;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.stabilizer.common.AgentAddress;
import com.hazelcast.stabilizer.common.AgentsFile;
import com.hazelcast.stabilizer.common.StabilizerProperties;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import static com.hazelcast.stabilizer.Utils.appendText;
import static com.hazelcast.stabilizer.Utils.sleepMillis;


/*
* An aws specific provisioning class which is using the AWS sdk to create aws instances,  and aws elastic load balance
*
*
* */
public class AwsProvisioner {

    private final static ILogger log = Logger.getLogger(AwsProvisioner.class);

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

    private StabilizerProperties props = new StabilizerProperties();


    private final File agentsFile = new File("agents.txt");
    private final File elbFile = new File(AWS_ELB_FILE_NAME);

    private File credentialsFile;

    private String securityGroup;

    private String awsKeyName;
    private String awsAmi;
    private String awsBoxId;

    private String elbProtocol;
    private int elbPortIn;
    private int elbPortOut;
    private String elbAzs;


    public AwsProvisioner() throws IOException {
        props.init(null);

        String awsCredentialsPath = props.get("AWS_CREDENTIALS", "AwsCredentials.properties");
        credentialsFile = new File(awsCredentialsPath);

        elbProtocol = props.get("ELB_PROTOCOL");

        elbPortIn = Integer.parseInt(props.get("ELB_PORT_IN"));
        elbPortOut = Integer.parseInt(props.get("ELB_PORT_OUT"));
        elbAzs = props.get("ELB_AZS");

        awsKeyName = props.get("AWS_KEY_NAME");
        awsAmi = props.get("AWS_AMI");
        awsBoxId = props.get("AWS_BOXID");
        securityGroup = props.get("SECURITY_GROUP");


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


    public List<Instance> checkAgentsFileCreateInstancesIfNeeded(int totalInstancesWanted){
        List agents = AgentsFile.load(agentsFile);

        if(totalInstancesWanted <= agents.size()){
            log.info("Currently decreasing instances is not supported");
            return Collections.EMPTY_LIST;
        }
        int instanceCount= totalInstancesWanted - agents.size();

        return createInstances(instanceCount);
    }


    public List<Instance> createInstances(int instanceCount){

        RunInstancesRequest runInstancesRequest = new RunInstancesRequest();

        runInstancesRequest.withImageId(awsAmi)
                .withInstanceType(awsBoxId)
                .withMinCount(instanceCount)
                .withMaxCount(instanceCount)
                .withKeyName(awsKeyName)
                .withSecurityGroups(securityGroup);

        RunInstancesResult runInstancesResult = ec2.runInstances(runInstancesRequest);

        List<Instance> checkedInstances = new ArrayList();
        List<Instance> instances = runInstancesResult.getReservation().getInstances();
        for (Instance i : instances) {

            if(waiteForInstanceStatus(i)){
                addInstanceToAgentsFile(i);
                checkedInstances.add(i);
            }else{
                log.warning("Time out Waiting for Instance Status id=" + i.getInstanceId() );
            }
        }
        return checkedInstances;
    }

    public List<Instance> getInstancesFromAgentsFile(){

        DescribeInstancesRequest request = new DescribeInstancesRequest();
        List<String> ips = new ArrayList<String>();


        List<AgentAddress> currentAgents = AgentsFile.load(agentsFile);

        for(AgentAddress agent : currentAgents){
            ips.add(agent.publicAddress);
        }

        Filter filter = new Filter(AWS_PUBLIC_IP_FILTER, ips);

        DescribeInstancesResult result = ec2.describeInstances(request.withFilters(filter));
        List<Reservation> reservations = result.getReservations();

        List<Instance> foundInstances = new ArrayList();

        for (Reservation reservation : reservations) {
            List<Instance> instances = reservation.getInstances();
            foundInstances.addAll(instances);

            for (Instance instance : instances) {

                log.info("found id=" + instance.getInstanceId());

            }
        }
        return foundInstances;
    }

    public void findAwsInstanceWithKeyName(String keyName){

        File found = new File("found.txt");

        DescribeInstancesRequest request = new DescribeInstancesRequest();
        List<String> keyNames = new ArrayList<String>();
        keyNames.add(keyName);

        Filter filter = new Filter(AWS_KET_NAME_FILTER, keyNames);

        DescribeInstancesResult result = ec2.describeInstances(request.withFilters(filter));
        List<Reservation> reservations = result.getReservations();

        List<Instance> foundInstances = new ArrayList();

        for (Reservation reservation : reservations) {
            List<Instance> instances = reservation.getInstances();
            foundInstances.addAll(instances);

            for (Instance i : instances) {
                appendText(i.getPublicIpAddress() + "," + i.getPrivateIpAddress() + "\n", found);
            }
        }
    }

    public boolean waiteForInstanceStatus(Instance i){

        DescribeInstanceStatusRequest describeStatusRequest = new DescribeInstanceStatusRequest().withInstanceIds(i.getInstanceId());
        int counter=0;
        while ( counter++ < MAX_SLEEPING_ITTERATIONS ) {
            sleepMillis(SLEEPING_MS);

            DescribeInstanceStatusResult statusRes = ec2.describeInstanceStatus(describeStatusRequest);
            List<InstanceStatus> statusList = statusRes.getInstanceStatuses();

            for ( InstanceStatus s : statusList ) {
                String systemStatus = s.getSystemStatus().getStatus();
                if(systemStatus.equals(statusList)){
                    return true;
                }
            }
        }


        /*
        DescribeInstancesRequest describeInstancesRequest = new DescribeInstancesRequest().withInstanceIds(i.getInstanceId());
        int counter=0;
        while ( counter++ < MAX_SLEEPING_ITTERATIONS ) {
            sleepMillis(SLEEPING_MS);

            DescribeInstancesResult result = ec2.describeInstances(describeInstancesRequest);

            for ( Reservation r : result.getReservations() ) {
                for ( Instance j : r.getInstances() ) {

                    log.warning("Time out Waiting for Instance Status id=" + j.getInstanceId() + " " + j.getPublicIpAddress() + ", " + j.getState().getName());
                    if ( j.getPublicIpAddress() != null  && j.getState().getName().equals(AWS_RUNNING_STATUS) ) {

                        return true;
                    }
                }
            }
        }
        */

        return false;
    }

    public void addInstanceToAgentsFile(Instance i){
        DescribeInstancesRequest describeInstancesRequest = new DescribeInstancesRequest().withInstanceIds(i.getInstanceId());
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
        lbRequest.withAvailabilityZones(elbAzs.split(","));

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
            log.warning(e);
        }
        return false;
    }

    public void addInstancesToElb(String name, List<Instance> instances){
        if(instances.isEmpty()){
            log.info("No instances to add to load balance "+name);
            return;
        }

        //get instance id's
        Collection ids = new ArrayList();
        for (Instance i: instances) {
            ids.add(new com.amazonaws.services.elasticloadbalancing.model.Instance(i.getInstanceId()));
        }

        //register the instances to the balancer
        RegisterInstancesWithLoadBalancerRequest register = new RegisterInstancesWithLoadBalancerRequest();
        register.setLoadBalancerName(name);
        register.setInstances(ids);
        RegisterInstancesWithLoadBalancerResult registerWithLoadBalancerResult = elb.registerInstancesWithLoadBalancer(register);
    }
}
