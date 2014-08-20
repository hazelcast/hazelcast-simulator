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
package com.hazelcast.stabilizer.tests.map;

import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.Member;
import com.hazelcast.core.Partition;
import com.hazelcast.core.PartitionService;
import com.hazelcast.instance.HazelcastInstanceProxy;
import com.hazelcast.stabilizer.tests.TestContext;
import com.hazelcast.stabilizer.tests.TestRunner;
import com.hazelcast.stabilizer.tests.annotations.Run;
import com.hazelcast.stabilizer.tests.annotations.Setup;
import com.hazelcast.stabilizer.tests.annotations.Verify;
import com.hazelcast.stabilizer.tests.annotations.Warmup;
import com.hazelcast.stabilizer.tests.utils.ThreadSpawner;

import java.util.concurrent.TimeUnit;


public class MapTTLSaturationTest {

    public String basename = this.getClass().getName();
    public int threadCount = 3;
    public int ttlHours = 24;

    private TestContext testContext;
    private HazelcastInstance targetInstance;

    public double aproxHeapUsageFactor = 0.9;
    //private long aproxEntryBytesSize = 238;

    private IMap map;

    private long baseLineUsed;

    public MapTTLSaturationTest(){
    }

    @Setup
    public void setup(TestContext testContext) throws Exception {
        this.testContext = testContext;
        targetInstance = testContext.getTargetInstance();

        map = targetInstance.getMap(basename);
    }

    @Warmup(global = false)
    public void warmup() {
        if(isMemberNode()){
            printMemStats();

            //long maxLocalEntries = (long) ( (maxBytes / aproxEntryBytesSize) * aproxHeapUsageFactor) ;

            long key=0;
            long putCount=0;
            while(heapUsedFactor() < aproxHeapUsageFactor){
                key = nextKeyOwnedby(key);
                map.put(key, key, ttlHours, TimeUnit.HOURS);
                key++;
                putCount++;
            }
            System.out.println(basename+" putCount = "+putCount);

            printMemStats();
            //System.out.println(basename+" avgEntryBytes = "+avgEntryBytes+" vs "+putCount+" estimate used");
        }
    }

    @Run
    public void run() {
        ThreadSpawner spawner = new ThreadSpawner(testContext.getTestId());
        for (int k = 0; k < threadCount; k++) {
            spawner.spawn(new Worker());
        }
        spawner.awaitCompletion();
    }

    private class Worker implements Runnable {
        @Override
        public void run() {
            while (!testContext.isStopped()) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    @Verify(global = false)
    public void loaclVerify() throws Exception {

        if(isMemberNode()){
            printMemStats();
        }

    }

    private double heapUsedFactor() {
        long total = Runtime.getRuntime().totalMemory();
        long free = Runtime.getRuntime().freeMemory();
        long used = total - free;
        long max = Runtime.getRuntime().maxMemory();
        return used / max;
    }

    public void printMemStats(){

        long free = Runtime.getRuntime().freeMemory();
        long total =  Runtime.getRuntime().totalMemory();
        long used = total - free;
        long maxBytes =  Runtime.getRuntime().maxMemory();
        double usedOfMax = 100.0 * ( (double) used / (double) maxBytes);

        System.out.println(basename+" before Init");
        System.out.println(basename+" free = "+humanReadableByteCount(free, true)+" = "+free);
        System.out.println(basename+" used = "+humanReadableByteCount(used, true)+" = "+ used);
        System.out.println(basename+" max = "+humanReadableByteCount(maxBytes, true)+" = "+maxBytes);
        System.out.println(basename+" usedOfMax = "+usedOfMax+"%");

    }

    public static String humanReadableByteCount(long bytes, boolean si) {
        int unit = si ? 1000 : 1024;
        if (bytes < unit) return bytes + " B";
        int exp = (int) (Math.log(bytes) / Math.log(unit));
        String pre = (si ? "kMGTPE" : "KMGTPE").charAt(exp-1) + (si ? "" : "i");
        return String.format("%.1f %sB", bytes / Math.pow(unit, exp), pre);
    }

    public  long nextKeyOwnedby(long key) {
        final Member localMember = targetInstance.getCluster().getLocalMember();
        final PartitionService partitionService = targetInstance.getPartitionService();
        for ( ; ; ) {

            Partition partition = partitionService.getPartition(key);
            if (localMember.equals(partition.getOwner())) {
                return key;
            }
            key++;
        }
    }

    public boolean isMemberNode(){
        return targetInstance instanceof HazelcastInstanceProxy;
    }
}
