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
package com.hazelcast.simulator.tests.external;

import com.hazelcast.simulator.common.SimulatorProperties;
import com.hazelcast.simulator.test.annotations.Run;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.AbstractTest;
import com.hazelcast.simulator.utils.Bash;

import static com.hazelcast.simulator.tests.helpers.HazelcastTestUtils.isClient;
import static com.hazelcast.simulator.tests.helpers.HazelcastTestUtils.isMemberNode;
import static com.hazelcast.simulator.utils.FileUtils.deleteQuiet;
import static com.hazelcast.simulator.utils.UuidUtil.newSecureUuidString;
import static java.lang.String.format;

public class ExternalClientStarterTest extends AbstractTest {

    // properties
    public String binaryName = "binaryName";
    public String arguments = "";
    public String logFileName = "external-client";
    public int processCount = 1;

    private final SimulatorProperties properties = new SimulatorProperties();
    private final Bash bash = new Bash(properties);

    private String ipAddress;

    @Setup
    public void setUp() {
        if (isClient(targetInstance)) {
            targetInstance.getAtomicLong("externalClientsStarted").addAndGet(processCount);
        }
        ipAddress = testContext.getPublicIpAddress();

        // delete the local binary, so it won't get downloaded again
        deleteQuiet(binaryName);
    }

    @Run
    public void run() {
        if (isMemberNode(targetInstance)) {
            return;
        }

        for (int i = 1; i <= processCount; i++) {
            String tmpArguments = arguments
                    .replace("$PROCESS_INDEX", String.valueOf(i))
                    .replace("$IP_ADDRESS", ipAddress)
                    .replace("$UUID", newSecureUuidString());

            String tmpLogFileName = logFileName + '_' + i;

            logger.info(format("Starting external client: %s %s &>> %s.log", binaryName, tmpArguments, tmpLogFileName));
            bash.execute(format("../upload/%s %s &>> %s.log &", binaryName, tmpArguments, tmpLogFileName));
        }
    }
}
