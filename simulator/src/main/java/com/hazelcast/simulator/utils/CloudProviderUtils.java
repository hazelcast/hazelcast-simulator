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
package com.hazelcast.simulator.utils;

import com.hazelcast.simulator.common.RunMode;
import com.hazelcast.simulator.common.SimulatorProperties;

public final class CloudProviderUtils {

    public static final String PROVIDER_LOCAL = "local";
    public static final String PROVIDER_STATIC = "static";
    public static final String PROVIDER_EC2 = "aws-ec2";
    public static final String PROVIDER_GCE = "google-compute-engine";

    private CloudProviderUtils() {
    }

    public static boolean isCloudProvider(SimulatorProperties properties) {
        return (!isLocal(properties) && !isStatic(properties));
    }

    public static boolean isLocal(SimulatorProperties properties) {
        return isLocal(properties.getCloudProvider());
    }

    public static RunMode runMode(SimulatorProperties properties) {
        String provider = properties.getCloudProvider();
        if (provider.equals("embedded")) {
            return RunMode.Embedded;
        } else if (provider.equals("local")) {
            return RunMode.Local;
        } else {
            return RunMode.Remote;
        }
    }

    public static boolean isTrueCloud(String cloudProvider) {
        return !cloudProvider.equals("embedded")
                && !cloudProvider.equals("local")
                && !cloudProvider.equals("static");
    }

    public static boolean isStatic(SimulatorProperties properties) {
        return isStatic(properties.getCloudProvider());
    }

    public static boolean isEC2(SimulatorProperties properties) {
        return isEC2(properties.getCloudProvider());
    }

    public static boolean isGCE(SimulatorProperties properties) {
        return isGCE(properties.getCloudProvider());
    }

    public static boolean isLocal(String cloudProvider) {
        return PROVIDER_LOCAL.equals(cloudProvider);
    }

    public static boolean isStatic(String cloudProvider) {
        return PROVIDER_STATIC.equals(cloudProvider);
    }

    public static boolean isEC2(String cloudProvider) {
        return PROVIDER_EC2.equals(cloudProvider);
    }

    public static boolean isGCE(String cloudProvider) {
        return PROVIDER_GCE.equals(cloudProvider);
    }
}
