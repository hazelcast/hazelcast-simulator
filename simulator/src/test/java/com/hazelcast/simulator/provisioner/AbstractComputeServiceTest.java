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

import org.jclouds.aws.ec2.AWSEC2Api;
import org.jclouds.aws.ec2.compute.AWSEC2TemplateOptions;
import org.jclouds.aws.ec2.features.AWSSecurityGroupApi;
import org.jclouds.compute.ComputeService;
import org.jclouds.compute.ComputeServiceContext;
import org.jclouds.compute.domain.Hardware;
import org.jclouds.compute.domain.Image;
import org.jclouds.compute.domain.Template;
import org.jclouds.compute.domain.TemplateBuilderSpec;
import org.jclouds.compute.domain.Volume;
import org.jclouds.compute.domain.internal.TemplateImpl;
import org.jclouds.domain.Location;
import org.jclouds.ec2.domain.SecurityGroup;

import java.util.HashSet;
import java.util.Set;

import static java.util.Collections.singletonList;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public abstract class AbstractComputeServiceTest {

    static final String SECURITY_GROUP = "simulator";

    Set<SecurityGroup> securityGroups = new HashSet<SecurityGroup>();

    AWSSecurityGroupApi securityGroupApi;
    ComputeService computeService;

    void initComputeServiceMock() {
        Volume volume = mock(Volume.class);
        when(volume.isBootDevice()).thenReturn(false);
        when(volume.getType()).thenReturn(Volume.Type.LOCAL);
        when(volume.getDevice()).thenReturn("/dev/sdb");
        when(volume.getSize()).thenReturn(160f);

        Hardware hardware = mock(Hardware.class);
        doReturn(singletonList(volume)).when(hardware).getVolumes();

        Image image = mock(Image.class);
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
