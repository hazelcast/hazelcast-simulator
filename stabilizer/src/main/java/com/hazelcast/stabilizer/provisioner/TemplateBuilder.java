package com.hazelcast.stabilizer.provisioner;

import com.hazelcast.stabilizer.agent.remoting.AgentRemoteService;
import com.hazelcast.stabilizer.agent.workerjvm.WorkerJvmManager;
import com.hazelcast.stabilizer.common.StabilizerProperties;
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

import static com.hazelcast.stabilizer.utils.CommonUtils.exitWithError;

public class TemplateBuilder {
    private final static Logger log = Logger.getLogger(Provisioner.class);

    private final ComputeService compute;
    private final StabilizerProperties props;
    private String machineSpec;
    private String securityGroup;
    private TemplateBuilderSpec spec;

    public TemplateBuilder(ComputeService compute, StabilizerProperties properties) {
        this.compute = compute;
        this.props = properties;
    }

    public Template build() {
        machineSpec = props.get("MACHINE_SPEC", "");

        log.info("Machine spec: " + machineSpec);

        securityGroup = props.get("SECURITY_GROUP", "stabilizer");

        spec = TemplateBuilderSpec.parse(machineSpec);

        Template template = buildTemplate();

        log.info("Created template");

        String user = props.get("USER", "stabilizer");
        AdminAccess adminAccess = AdminAccess.builder().adminUsername(user).build();

        log.info("Loginname to the remote machines: " + user);

        template.getOptions()
                .inboundPorts(inboundPorts())
                .runScript(adminAccess);

        String subnetId = props.get("SUBNET_ID", "default");
        if (subnetId.equals("default") || subnetId.isEmpty()) {
            initSecurityGroup();
            template.getOptions().securityGroups(securityGroup);
        } else {
            if (!props.isEc2()) {
                throw new IllegalStateException("SUBNET_ID can be used only when EC2 is configured as a cloud provider.");
            }
            log.info("Using VPC, Subnet ID = " + subnetId);
            template.getOptions().as(AWSEC2TemplateOptions.class)
                    .subnetId(subnetId);
        }
        return template;
    }

    private Template buildTemplate() {
        try {
            return compute.templateBuilder()
                    .from(spec)
                    .build();
        } catch (IllegalArgumentException e) {
            log.debug(e);
            exitWithError(log, e.getMessage());
            return null;
        }
    }

    private int[] inboundPorts() {
        List<Integer> ports = new ArrayList<Integer>();
        ports.add(22);
        ports.add(AgentRemoteService.PORT);
        ports.add(WorkerJvmManager.PORT);
        for (int k = 5701; k < 5751; k++) {
            ports.add(k);
        }

        int[] result = new int[ports.size()];
        for (int k = 0; k < result.length; k++) {
            result[k] = ports.get(k);
        }
        return result;
    }

    private void initSecurityGroup() {
        if (!props.isEc2()) {
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
            log.info("Security group: '" + securityGroup + "' is found in region '" + region + "'");
            return;
        }

        log.info("Security group: '" + securityGroup + "' is not found in region '" + region + "', creating it on the fly");

        securityGroupApi.createSecurityGroupInRegion(region, securityGroup, securityGroup);

        // this duplication of ports is ugly since we already do it in 'inboundPorts method'
        securityGroupApi.authorizeSecurityGroupIngressInRegion(region, securityGroup, IpProtocol.TCP, 22, 22, "0.0.0.0/0");
        securityGroupApi.authorizeSecurityGroupIngressInRegion(region, securityGroup, IpProtocol.TCP, 9000, 9001, "0.0.0.0/0");
        securityGroupApi.authorizeSecurityGroupIngressInRegion(region, securityGroup, IpProtocol.TCP, 5701, 5751, "0.0.0.0/0");
    }
}
