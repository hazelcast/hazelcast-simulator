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
package com.hazelcast.simulator.tests.helpers;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.BuildInfo;
import com.hazelcast.instance.BuildInfoProvider;
import com.hazelcast.simulator.test.TestException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


import static com.hazelcast.simulator.utils.CommonUtils.sleepSeconds;
import static com.hazelcast.simulator.utils.VersionUtils.isMinVersion;
import static java.lang.String.format;
import static org.junit.Assert.fail;

public final class HazelcastTestUtils {

    private static final Logger LOGGER = LogManager.getLogger(HazelcastTestUtils.class);

    private HazelcastTestUtils() {
    }

    public static RuntimeException rethrow(Throwable throwable) {
        if (throwable instanceof RuntimeException) {
            throw (RuntimeException) throwable;
        } else {
            throw new TestException(throwable);
        }
    }

    public static void waitClusterSize(Logger logger, HazelcastInstance hz, int clusterSize) {
        for (; ; ) {
            if (hz.getCluster().getMembers().size() >= clusterSize) {
                return;
            }

            logger.info("waiting cluster == " + clusterSize);
            sleepSeconds(1);
        }
    }

    public static void failOnVersionMismatch(String minVersion, String message) {
        BuildInfo buildInfo = BuildInfoProvider.getBuildInfo();
        String actualVersion = buildInfo.getVersion();
        LOGGER.info("Compare version " + actualVersion + " with minimum version " + minVersion + ": "
                + isMinVersion(minVersion, actualVersion));
        if (!isMinVersion(minVersion, actualVersion)) {
            fail(format(message, minVersion));
        }
    }
}
