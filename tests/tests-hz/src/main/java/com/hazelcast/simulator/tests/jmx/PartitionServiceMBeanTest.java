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
package com.hazelcast.simulator.tests.jmx;

import com.hazelcast.simulator.test.AbstractTest;
import com.hazelcast.simulator.test.annotations.Prepare;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.TimeStep;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;

import static com.hazelcast.simulator.tests.helpers.HazelcastTestUtils.getOperationCountInformation;
import static com.hazelcast.simulator.tests.helpers.HazelcastTestUtils.waitClusterSize;

public class PartitionServiceMBeanTest extends AbstractTest {

    // properties
    public int minNumberOfMembers = 0;

    private MBeanServer mBeanServer;
    private ObjectName objectName;

    @Setup
    public void setUp() throws Exception {
        this.mBeanServer = ManagementFactory.getPlatformMBeanServer();
        this.objectName = new ObjectName("com.hazelcast:instance=" + targetInstance.getName()
                + ",name=" + targetInstance.getName() + ",type=HazelcastInstance.PartitionServiceMBean");
   }

    @Prepare(global = false)
    public void prepare() {
        waitClusterSize(logger, targetInstance, minNumberOfMembers);
    }

    @TimeStep(prob = -1)
    public void isClusterSafe() throws Exception {
        mBeanServer.getAttribute(objectName, "isClusterSafe");
    }

    @TimeStep(prob = 0)
    public void isLocalMemberSafe() throws Exception {
        mBeanServer.getAttribute(objectName, "isLocalMemberSafe");
    }

    @Teardown
    public void tearDown() {
        logger.info(getOperationCountInformation(targetInstance));
    }
}
