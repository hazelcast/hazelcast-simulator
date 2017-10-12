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
package com.hazelcast.simulator.utils;

import com.hazelcast.simulator.common.SimulatorProperties;
import org.apache.log4j.Logger;

import static com.hazelcast.simulator.utils.CloudProviderUtils.isEC2;
import static com.hazelcast.simulator.utils.CommonUtils.getSimulatorVersion;
import static java.lang.String.format;

public final class HarakiriMonitorUtils {

    private static final Logger LOGGER = Logger.getLogger(HarakiriMonitorUtils.class);

    private HarakiriMonitorUtils() {
    }

    public static boolean isHarakiriMonitorEnabled(SimulatorProperties properties) {
        return (isEC2(properties) && "true".equalsIgnoreCase(properties.get("HARAKIRI_MONITOR_ENABLED")));
    }

    public static String getStartHarakiriMonitorCommandOrNull(SimulatorProperties properties) {
        if (!isHarakiriMonitorEnabled(properties)) {
            if (isEC2(properties)) {
                LOGGER.info("HarakiriMonitor is not enabled");
            }
            return null;
        }

        String waitSeconds = properties.get("HARAKIRI_MONITOR_WAIT_SECONDS");
        LOGGER.info(format("HarakiriMonitor is enabled and will kill inactive EC2 instances after %s seconds", waitSeconds));
        return format(
                "nohup hazelcast-simulator-%s/bin/harakiri-monitor --cloudProvider %s --cloudIdentity %s --cloudCredential %s"
                        + " --waitSeconds %s > harakiri.out 2> harakiri.err < /dev/null &",
                getSimulatorVersion(),
                properties.getCloudProvider(),
                properties.getCloudIdentity(),
                properties.getCloudCredential(),
                waitSeconds);
    }
}
