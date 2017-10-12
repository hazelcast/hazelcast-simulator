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
package com.hazelcast.simulator.harakiri;

import com.hazelcast.simulator.utils.CommandLineExitException;
import org.apache.log4j.Logger;

import static com.hazelcast.simulator.utils.CloudProviderUtils.isEC2;
import static com.hazelcast.simulator.utils.CommonUtils.sleepSeconds;
import static com.hazelcast.simulator.utils.NativeUtils.execute;
import static java.lang.String.format;

/**
 * Responsible for terminating EC2 instances if they are not used to prevent running into a high bill.
 */
class HarakiriMonitor {

    private static final Logger LOGGER = Logger.getLogger(HarakiriMonitor.class);

    private final String cloudProvider;
    private final String command;
    private final int waitSeconds;

    public HarakiriMonitor(String cloudProvider, String cloudIdentity, String cloudCredential, int waitSeconds) {
        this(cloudProvider, getHarakiriCommand(cloudIdentity, cloudCredential), waitSeconds);
    }

    HarakiriMonitor(String cloudProvider, String command, int waitSeconds) {
        this.cloudProvider = cloudProvider;
        this.command = command;
        this.waitSeconds = waitSeconds;
    }

    void start() {
        if (!isEC2(cloudProvider)) {
            LOGGER.info("No Harakiri monitor is active: only on AWS-EC2 unused machines will be terminated.");
            return;
        }

        LOGGER.info(format("Harakiri monitor is active and will wait %d seconds to kill this instance", waitSeconds));
        sleepSeconds(waitSeconds);

        LOGGER.info("Trying to commit Harakiri once!");
        try {
            LOGGER.info("Harakiri command: " + command);
            execute(command);
        } catch (Exception e) {
            throw new CommandLineExitException("Failed to execute Harakiri", e);
        }
    }

    private static String getHarakiriCommand(String cloudIdentity, String cloudCredential) {
        return format("ec2-terminate-instances $(curl -s http://169.254.169.254/latest/meta-data/instance-id)"
                + " --aws-access-key %s --aws-secret-key %s", cloudIdentity, cloudCredential);
    }
}
