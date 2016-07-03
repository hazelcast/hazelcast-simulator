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
import com.hazelcast.simulator.test.annotations.RunWithWorker;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Verify;
import com.hazelcast.simulator.tests.AbstractTest;
import com.hazelcast.simulator.worker.tasks.AbstractMonotonicWorker;

import java.util.concurrent.TimeUnit;

import static com.hazelcast.simulator.utils.CommonUtils.sleepSeconds;
import static com.hazelcast.simulator.utils.FormatUtils.humanReadableByteCount;

public class MapTTLSaturationTest extends AbstractTest {

    // properties
    public double maxHeapUsagePercentage = 80;

    private IMap<Long, Long> map;

    @Setup
    public void setup() {
        map = targetInstance.getMap(basename);
    }

    @Verify(global = false)
    public void localVerify() {
        logHeapUsage("localVerify()");
    }

    @RunWithWorker
    public Worker createWorker() {
        return new Worker();
    }

    private class Worker extends AbstractMonotonicWorker {

        private int counter = 1;

        @Override
        public void beforeRun() {
            logHeapUsage("beforeRun()");
        }

        @Override
        protected void timeStep() throws Exception {
            double usedPercentage = heapUsedPercentage();
            if (usedPercentage >= maxHeapUsagePercentage) {
                logger.info(basename + " heap used: " + usedPercentage + " %, map size: " + map.size());

                sleepSeconds(10);
            } else {
                for (int i = 0; i < 1000; i++) {
                    counter++;
                    if (counter % 100000 == 0) {
                        logger.info(basename + " at: " + counter + ", heap used: " + usedPercentage
                                + " %, map size: " + map.size());
                    }
                    long key = getRandom().nextLong();
                    map.put(key, 0L, 24, TimeUnit.HOURS);
                }
            }
        }

        @Override
        public void afterRun() {
            logHeapUsage("afterRun()");
        }
    }

    private void logHeapUsage(String header) {
        long free = Runtime.getRuntime().freeMemory();
        long total = Runtime.getRuntime().totalMemory();
        long used = total - free;
        long max = Runtime.getRuntime().maxMemory();
        double usedOfMax = 100.0 * ((double) used / (double) max);

        logger.info(basename + ' ' + header);
        logger.info(basename + " map size: " + map.size());
        logger.info(basename + " free: " + humanReadableByteCount(free, true) + " = " + free);
        logger.info(basename + " used: " + humanReadableByteCount(used, true) + " = " + used);
        logger.info(basename + " max: " + humanReadableByteCount(max, true) + " = " + max);
        logger.info(basename + " usedOfMax: " + usedOfMax + '%');
    }

    private static double heapUsedPercentage() {
        long total = Runtime.getRuntime().totalMemory();
        long max = Runtime.getRuntime().maxMemory();
        return (100d * total) / max;
    }
}
