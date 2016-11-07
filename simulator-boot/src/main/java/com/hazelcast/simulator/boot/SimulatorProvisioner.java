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

package com.hazelcast.simulator.boot;

import com.hazelcast.simulator.common.SimulatorProperties;
import com.hazelcast.simulator.provisioner.Provisioner;
import com.hazelcast.simulator.utils.Bash;
import com.hazelcast.simulator.utils.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;

import static com.hazelcast.simulator.utils.SimulatorUtils.loadSimulatorProperties;

public class SimulatorProvisioner {

    private int machineCount;
    private SimulatorProperties simulatorProperties;

    public SimulatorProvisioner() {
        new SimulatorInstaller().install();
        this.simulatorProperties = loadSimulatorProperties();
        cloudProviderAwsEc2();
    }

    public SimulatorProvisioner cloudProviderAwsEc2() {
        return cloudProvider("aws-ec2");
    }

    public SimulatorProvisioner cloudProviderLocal() {
        return cloudProvider("local");
    }

    public SimulatorProvisioner cloudProvider(String cloudProvider) {
        simulatorProperties.set("CLOUD_PROVIDER", cloudProvider);
        return this;
    }

    public SimulatorProperties getSimulatorProperties() {
        return simulatorProperties;
    }

    public SimulatorProvisioner machineCount(int machineCount) {
        this.machineCount = machineCount;
        return this;
    }

    public SimulatorProvisioner cloudCredential(String cloudCredential) {
        stuff(simulatorProperties, "CLOUD_CREDENTIAL", cloudCredential);
        return this;
    }

    public SimulatorProvisioner cloudIdentity(String cloudIdentity) {
        stuff(simulatorProperties, "CLOUD_IDENTITY", cloudIdentity);
        return this;
    }

    public SimulatorProvisioner machineSpec(String machineSpec) {
        simulatorProperties.set("MACHINE_SPEC", machineSpec);
        return this;
    }

    private void stuff(SimulatorProperties simulatorProperties, String property, String value) {
        try {
            File file = File.createTempFile("xxxxxxxxx", "tmp");
            FileUtils.writeText(value, file);
            simulatorProperties.set(property, file.getAbsolutePath());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void provision() {
        Bash bash = new Bash(simulatorProperties);
        Provisioner provisioner = new Provisioner(simulatorProperties, bash);
        provisioner.scale(machineCount, new HashMap<String, String>());
    }
}
