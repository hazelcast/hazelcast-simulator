/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast.simulator.tests.concurrent.atomiclong;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.core.Partition;
import com.hazelcast.instance.Node;
import com.hazelcast.internal.partition.InternalPartition;
import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.Address;
import com.hazelcast.simulator.probes.Probe;
import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.test.TestRunner;
import com.hazelcast.simulator.test.annotations.InjectProbe;
import com.hazelcast.simulator.test.annotations.Run;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.Warmup;
import com.hazelcast.simulator.tests.helpers.KeyLocality;
import com.hazelcast.simulator.utils.ThreadSpawner;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.simulator.tests.helpers.HazelcastTestUtils.getNode;
import static com.hazelcast.simulator.tests.helpers.HazelcastTestUtils.getOperationCountInformation;
import static com.hazelcast.simulator.tests.helpers.KeyUtils.generateStringKeys;

public class AtomicLongOldTest {

    private static final ILogger log = Logger.getLogger(AtomicLongOldTest.class);

    //props
    public int countersLength = 1000;
    public int threadCount = 10;
    //  public int logFrequency = 10000;
    public int performanceUpdateFrequency = 1000;
    public String basename = "atomiclong";
    public KeyLocality keyLocality = KeyLocality.RANDOM;
    public int writePercentage = 100;
    public int warmupIterations = 100;

    //private IAtomicLong totalCounter;
    private IAtomicLong[] counters;
    private TestContext context;
    private HazelcastInstance targetInstance;
    private AtomicLong ops2 = new AtomicLong();
    private long startMs;
    private long endMs;

    @InjectProbe(useForThroughput = true)
    private Probe probe;

    @Setup
    public void setup(TestContext context) throws Exception {
        this.context = context;

        if (writePercentage < 0) {
            throw new IllegalArgumentException("Write percentage can't be smaller than 0");
        }

        if (writePercentage > 100) {
            throw new IllegalArgumentException("Write percentage can't be larger than 100");
        }

        targetInstance = context.getTargetInstance();

        // totalCounter = targetInstance.getAtomicLong(context.getTestId() + ":TotalCounter");
        counters = new IAtomicLong[countersLength];
        String[] names = generateStringKeys("", countersLength, keyLocality, context.getTargetInstance());

        for (int k = 0; k < counters.length; k++) {
            String key = names[k];
            counters[k] = targetInstance.getAtomicLong(key);
        }
    }

    @Warmup
    public void warmup() {
        for (int k = 0; k < warmupIterations; k++) {
            for (IAtomicLong counter : counters) {
                counter.get();
            }
        }
    }

    @Teardown
    public void teardown() throws Exception {
        for (IAtomicLong counter : counters) {
            counter.destroy();
        }
        log.warning("---Operations:" + ops2);
        log.warning("---Throughput:" + ((ops2.get() * 1000d) / (endMs - startMs)) + " ops/second");

        double local = 0;
        double remote = 0;
        Map<Partition, Integer> localCounts = new HashMap<Partition, Integer>();
        int maxCountPerPartition = 0;
        for (IAtomicLong counter : counters) {
            Partition partition = targetInstance.getPartitionService().getPartition(counter.getName());
            if (partition.getOwner().equals(targetInstance.getCluster().getLocalMember())) {
                local++;
                Integer countPerPartition = localCounts.get(partition);
                Integer newCountPerPartition = countPerPartition == null ? 1 : countPerPartition + 1;
                localCounts.put(partition, newCountPerPartition);

                if (newCountPerPartition > maxCountPerPartition) {
                    maxCountPerPartition = newCountPerPartition;
                }

            } else {
                remote++;
            }
        }

        int imbalanceViolations = 0;
        int minCountPerPartition = Integer.MAX_VALUE;
        for (Map.Entry<Partition, Integer> entry : localCounts.entrySet()) {
            if (entry.getValue() < maxCountPerPartition - 1) {
                imbalanceViolations++;
                log.warning("Imbalance max:" + maxCountPerPartition + " actual:" + entry.getValue());
            }

            if (entry.getValue() < minCountPerPartition) {
                minCountPerPartition = entry.getValue();
            }
        }

        double localPercentage = (local * 100) / counters.length;
        double remotePercentage = (remote * 100) / counters.length;
        log.warning("---localItemCount:" + local);
        log.warning("---remoteItemCount:" + remote);
        log.warning("---localPercentage:" + localPercentage + " %");
        log.warning("---remotePercentage:" + remotePercentage + " %");
        log.warning("---partitionCount:" + targetInstance.getPartitionService().getPartitions().size());
        log.warning("---maxCountPerPartition:" + maxCountPerPartition);
        log.warning("---minCountPerPartition:" + minCountPerPartition);
        log.warning("---imbalanceViolations:" + imbalanceViolations);

        long partitionOwnerConflicts = 0;
        Node node = getNode(targetInstance);
        InternalPartitionService partitionService = node.getPartitionService();
        for (IAtomicLong counter : counters) {
            int partitionId = partitionService.getPartitionId(counter.getName());
            InternalPartition partition = partitionService.getPartition(partitionId);
            Address address = partition.getReplicaAddress(0);
            if (!node.getClusterService().getLocalMember().getAddress().equals(address)) {
                partitionOwnerConflicts++;
            }
        }

        log.warning("---partitionOwnerConflicts:" + partitionOwnerConflicts);
        log.warning("---partitionOwnerConflictsPercentage:" + ((100d * partitionOwnerConflicts) / counters.length)+" %s");

        //totalCounter.destroy();
        log.info(getOperationCountInformation(targetInstance));
    }

    @Run
    public void run() {
        startMs = System.currentTimeMillis();
        ThreadSpawner spawner = new ThreadSpawner(context.getTestId());
        for (int k = 0; k < threadCount; k++) {
            spawner.spawn(new Worker());
        }
        spawner.awaitCompletion();
        endMs = System.currentTimeMillis();
    }
//
//    @Verify
//    public void verify() {
//        long expected = totalCounter.get();
//        long actual = 0;
//        for (IAtomicLong counter : counters) {
//            actual += counter.get();
//        }
//
//        assertEquals(expected, actual);
//    }

    private class Worker implements Runnable {
        private final Random random = new Random();

        @Override
        public void run() {
            long iteration = 0;
            long increments = 0;

            while (!context.isStopped()) {
                IAtomicLong counter = getRandomCounter();
                //       if (isWrite()) {
                //           increments++;
                //           counter.incrementAndGet();
                //       } else {
                counter.get();
                //      }

                iteration++;
//                if (iteration % logFrequency == 0) {
//                    log.info(Thread.currentThread().getName() + " At iteration: " + iteration);
//                }
                if (iteration % performanceUpdateFrequency == 0) {
                    probe.inc(performanceUpdateFrequency);
                }
            }
            ops2.addAndGet(iteration);
            probe.inc(iteration % performanceUpdateFrequency);
            //totalCounter.addAndGet(increments);
        }

        private boolean isWrite() {
            if (writePercentage == 100) {
                return true;
            } else if (writePercentage == 0) {
                return false;
            } else {
                return random.nextInt(100) <= writePercentage;
            }
        }

        private IAtomicLong getRandomCounter() {
            int index = random.nextInt(counters.length);
            return counters[index];
        }
    }

    public static void main(String[] args) throws Throwable {
        AtomicLongOldTest test = new AtomicLongOldTest();
        new TestRunner<AtomicLongOldTest>(test).withDuration(10).run();
    }
}

