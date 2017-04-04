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
package com.hazelcast.simulator.tests.map;

import com.hazelcast.core.IMap;
import com.hazelcast.simulator.hz.HazelcastTest;
import com.hazelcast.simulator.test.BaseThreadState;
import com.hazelcast.simulator.test.annotations.AfterRun;
import com.hazelcast.simulator.test.annotations.BeforeRun;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.simulator.test.annotations.Verify;

import static com.hazelcast.simulator.utils.CommonUtils.sleepSeconds;
import static com.hazelcast.simulator.utils.FormatUtils.humanReadableByteCount;
import static java.util.concurrent.TimeUnit.HOURS;

public class MapTTLSaturationTest extends HazelcastTest {

    // properties
    public double maxHeapUsagePercentage = 80;

    private IMap<Long, Long> map;

    @Setup
    public void setup() {
        map = targetInstance.getMap(name);
    }

    @BeforeRun
    public void beforeRun() {
        logHeapUsage("beforeRun()");
    }

    @TimeStep
    public void timeStep(ThreadState state) throws Exception {
        double usedPercentage = heapUsedPercentage();
        if (usedPercentage >= maxHeapUsagePercentage) {
            logger.info(name + " heap used: " + usedPercentage + " %, map size: " + map.size());

            sleepSeconds(10);
        } else {
            for (int i = 0; i < 1000; i++) {
                state.counter++;
                if (state.counter % 100000 == 0) {
                    logger.info(name + " at: " + state.counter + ", heap used: " + usedPercentage
                            + " %, map size: " + map.size());
                }
                long key = state.randomLong();
                map.put(key, 0L, 24, HOURS);
            }
        }
    }

    @AfterRun
    public void afterRun() {
        logHeapUsage("afterRun()");
    }

    public class ThreadState extends BaseThreadState {
        private int counter = 1;
    }

    private void logHeapUsage(String header) {
        long free = Runtime.getRuntime().freeMemory();
        long total = Runtime.getRuntime().totalMemory();
        long used = total - free;
        long max = Runtime.getRuntime().maxMemory();
        double usedOfMax = 100.0 * ((double) used / (double) max);

        logger.info(name + ' ' + header);
        logger.info(name + " map size: " + map.size());
        logger.info(name + " free: " + humanReadableByteCount(free, true) + " = " + free);
        logger.info(name + " used: " + humanReadableByteCount(used, true) + " = " + used);
        logger.info(name + " max: " + humanReadableByteCount(max, true) + " = " + max);
        logger.info(name + " usedOfMax: " + usedOfMax + '%');
    }

    private static double heapUsedPercentage() {
        long total = Runtime.getRuntime().totalMemory();
        long max = Runtime.getRuntime().maxMemory();
        return (100d * total) / max;
    }


    @Verify(global = false)
    public void localVerify() {
        logHeapUsage("localVerify()");
    }
}
