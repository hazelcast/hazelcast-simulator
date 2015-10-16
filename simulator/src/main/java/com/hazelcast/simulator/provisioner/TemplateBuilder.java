package com.hazelcast.simulator.provisioner;

import com.hazelcast.simulator.common.SimulatorProperties;
import com.hazelcast.simulator.protocol.configuration.Ports;
import org.apache.log4j.Logger;
import org.jclouds.aws.ec2.AWSEC2Api;
import org.jclouds.aws.ec2.compute.AWSEC2TemplateOptions;
import org.jclouds.compute.ComputeService;
import org.jclouds.compute.domain.Template;
import org.jclouds.compute.domain.TemplateBuilderSpec;
import org.jclouds.ec2.domain.SecurityGroup;
import org.jclouds.ec2.features.SecurityGroupApi;
import org.jclouds.net.domain.IpProtocol;
import org.jclouds.scriptbuilder.statements.login.AdminAccess;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static com.hazelcast.simulator.utils.CloudProviderUtils.isEC2;

class TemplateBuilder {

    private static final int SSH_PORT = 22;
    private static final int HAZELCAST_PORT_RANGE_START = 5701;
    private static final int HAZELCAST_PORT_RANGE_END = 5751;
    private static final String CIDR_RANGE = "0.0.0.0/0";

    private static final Logger LOGGER = Logger.getLogger(Provisioner.class);

    private final ComputeService compute;
    private final SimulatorProperties props;
    private String securityGroup;
    private TemplateBuilderSpec spec;

    TemplateBuilder(ComputeService compute, SimulatorProperties properties) {
        this.compute = compute;
        this.props = properties;
    }

    Template build() {
        securityGroup = props.get("SECURITY_GROUP", "simulator");

        String machineSpec = props.get("MACHINE_SPEC", "");
        spec = TemplateBuilderSpec.parse(machineSpec);
        LOGGER.info("Machine spec: " + machineSpec);

        Template template = buildTemplate();
        LOGGER.info("Created template");

        String user = props.get("USER", "simulator");
        AdminAccess adminAccess = AdminAccess.builder().adminUsername(user).build();
        LOGGER.info("Login name to the remote machines: " + user);

        template.getOptions()
                .inboundPorts(inboundPorts())
                .runScript(adminAccess);

        String subnetId = props.get("SUBNET_ID", "default");
        if (subnetId.equals("default") || subnetId.isEmpty()) {
            initSecurityGroup();
            template.getOptions().securityGroups(securityGroup);
        } else {
            if (!isEC2(props.get("CLOUD_PROVIDER"))) {
                throw new IllegalStateException("SUBNET_ID can be used only when EC2 is configured as a cloud provider.");
            }
            LOGGER.info("Using VPC, Subnet ID = " + subnetId);
            template.getOptions()
                    .as(AWSEC2TemplateOptions.class)
                    .subnetId(subnetId);
        }
        return template;
    }

    private Template buildTemplate() {
        return compute.templateBuilder().from(spec).build();
    }

    private int[] inboundPorts() {
        List<Integer> ports = new ArrayList<Integer>();
        ports.add(SSH_PORT);
        ports.add(Ports.AGENT_PORT);
        for (int port = HAZELCAST_PORT_RANGE_START; port < HAZELCAST_PORT_RANGE_END; port++) {
            ports.add(port);
        }

        int[] result = new int[ports.size()];
        for (int i = 0; i < result.length; i++) {
            result[i] = ports.get(i);
        }
        return result;
    }

    private void initSecurityGroup() {
        if (!isEC2(props.get("CLOUD_PROVIDER"))) {
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
            LOGGER.info("Security group: '" + securityGroup + "' is found in region '" + region + "'");
            return;
        }

        LOGGER.info("Security group: '" + securityGroup + "' is not found in region '" + region + "', creating it on the fly");

        securityGroupApi.createSecurityGroupInRegion(region, securityGroup, securityGroup);

        // this duplication of ports is ugly since we already do it in 'inboundPorts method'
        securityGroupApi.authorizeSecurityGroupIngressInRegion(region, securityGroup, IpProtocol.TCP,
                SSH_PORT, SSH_PORT, CIDR_RANGE);
        securityGroupApi.authorizeSecurityGroupIngressInRegion(region, securityGroup, IpProtocol.TCP,
                Ports.AGENT_PORT, Ports.AGENT_PORT, CIDR_RANGE);
        securityGroupApi.authorizeSecurityGroupIngressInRegion(region, securityGroup, IpProtocol.TCP,
                HAZELCAST_PORT_RANGE_START, HAZELCAST_PORT_RANGE_END, CIDR_RANGE);
    }
}
