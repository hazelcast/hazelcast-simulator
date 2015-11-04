package com.hazelcast.simulator.provisioner;

import org.jclouds.aws.ec2.AWSEC2Api;
import org.jclouds.aws.ec2.compute.AWSEC2TemplateOptions;
import org.jclouds.aws.ec2.features.AWSSecurityGroupApi;
import org.jclouds.compute.ComputeService;
import org.jclouds.compute.ComputeServiceContext;
import org.jclouds.compute.domain.Hardware;
import org.jclouds.compute.domain.Image;
import org.jclouds.compute.domain.Template;
import org.jclouds.compute.domain.TemplateBuilderSpec;
import org.jclouds.compute.domain.internal.TemplateImpl;
import org.jclouds.domain.Location;
import org.jclouds.ec2.domain.SecurityGroup;

import java.util.HashSet;
import java.util.Set;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public abstract class AbstractComputeServiceTest {

    static final String SECURITY_GROUP = "simulator";

    Set<SecurityGroup> securityGroups = new HashSet<SecurityGroup>();

    AWSSecurityGroupApi securityGroupApi;
    ComputeService computeService;

    void initComputeServiceMock() {
        Image image = mock(Image.class);
        Hardware hardware = mock(Hardware.class);
        Location location = mock(Location.class);
        AWSEC2TemplateOptions templateOptions = new AWSEC2TemplateOptions();

        Template template = new TemplateImpl(image, hardware, location, templateOptions);

        securityGroupApi = mock(AWSSecurityGroupApi.class);
        when(securityGroupApi.describeSecurityGroupsInRegion(anyString(), eq(SECURITY_GROUP))).thenReturn(securityGroups);

        AWSEC2Api ec2Api = mock(AWSEC2Api.class, RETURNS_DEEP_STUBS);
        when(ec2Api.getSecurityGroupApi().get()).thenReturn(securityGroupApi);

        ComputeServiceContext computeServiceContext = mock(ComputeServiceContext.class);
        when(computeServiceContext.unwrapApi(AWSEC2Api.class)).thenReturn(ec2Api);

        computeService = mock(ComputeService.class, RETURNS_DEEP_STUBS);
        when(computeService.templateBuilder().from(any(TemplateBuilderSpec.class)).build()).thenReturn(template);
        when(computeService.getContext()).thenReturn(computeServiceContext);
    }
}
