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

import com.hazelcast.core.*;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.map.EntryBackupProcessor;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.stabilizer.Utils;
import com.hazelcast.stabilizer.tests.TestContext;
import com.hazelcast.stabilizer.tests.TestRunner;
import com.hazelcast.stabilizer.tests.annotations.*;
import com.hazelcast.stabilizer.tests.map.helpers.ScrambledZipfianGenerator;
import com.hazelcast.stabilizer.tests.utils.ThreadSpawner;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class MapUsageStressTest {

    private final static ILogger log = Logger.getLogger(MapUsageStressTest.class);
    private final static String alphabet = "abcdefghijklmnopqrstuvwxyz1234567890";

    private String basename = this.getClass().getName();
    public int threadCount = 10;
    public int valueLength = 100;
    public int keyCount = 1000;
    public int valueCount = 1000;
    public int maxMaps = 1;

    public boolean randomDistributionUniform=false;

    //add up to 1
    public double writeProb = 0.4;
    public double getProb = 0.2;

    public double clearProb = 0;
    public double replaceProb = 0.15;
    public double removeProb = 0.1;
    public double exicuteOnProb = 0.15;
    //

    //add up to 1   (writeProb is splitup into sub styles)
    public double writeUsingPutProb = 0.15;
    public double writeUsingPutIfAbsent = 0.15;
    public double writeUsingPutExpireProb = 0.4;
    public double lockAndMod = 0.3;
    //

    public int minExpireMillis = 200;
    public int maxExpireMillis = 8000;

    public int migrationListenerCount = 1;
    public int migrationListenerDelayMills = 40;

    public int membershipListenerCount = 1;
    public int membershipListenerDelayMills = 30;

    public int lifecycleListenerCount = 1;
    public int lifecycleListenerDelayMills = 20;

    public int distributedObjectListenerCount = 1;
    public int distributedObjectListenerDelayMills = 10;

    public int localMapEntryListenerCount = 0;
    public int localMapEntryListenerDelayMills = 0;

    public int mapEntryListenerCount = 1;
    public int mapEntryListenerDelayMills = 0;

    public int entryProcessorDelayMills = 100;


    private String[] values;
    private TestContext testContext;
    private HazelcastInstance targetInstance;

    private List listeners = new ArrayList();

    ScrambledZipfianGenerator mapsZipfian = new ScrambledZipfianGenerator(maxMaps);
    ScrambledZipfianGenerator kesyZipfian = new ScrambledZipfianGenerator(keyCount);

    @Setup
    public void setup(TestContext testContext) throws Exception {
        this.testContext = testContext;
        targetInstance = testContext.getTargetInstance();

        for (int k = 0; k < migrationListenerCount; k++) {
            MigrationnListenerImpl l = new MigrationnListenerImpl();
            listeners.add(l);

            try{
                targetInstance.getPartitionService().addMigrationListener(l);
            }catch(UnsupportedOperationException e){}
        }

        for (int k = 0; k < membershipListenerCount; k++) {
            MembershipListenerImpl l = new MembershipListenerImpl();
            listeners.add(l);
            targetInstance.getCluster().addMembershipListener(l);
        }

        for (int k = 0; k < lifecycleListenerCount; k++) {
            LifecycleListenerImpl l = new LifecycleListenerImpl();
            listeners.add(l);
            targetInstance.getLifecycleService().addLifecycleListener(l);
        }

        for (int k = 0; k < distributedObjectListenerCount; k++) {
            DistributedObjectListenerImpl l = new DistributedObjectListenerImpl();
            listeners.add(l);
            targetInstance.addDistributedObjectListener(l);
        }
    }

    //happens just on one machien.
    @Warmup
    public void warmup() {
        log.info("===WARMUP===");

        values = new String[valueCount];


        for (int k = 0; k < values.length; k++) {
            values[k] = makeString(valueLength);
        }

        for (int i = 0; i < maxMaps; i++) {
            IMap map = targetInstance.getMap(basename + i);

            for (int count = 0; count < mapEntryListenerCount; count++) {
                EntryListenerImpl l = new EntryListenerImpl(mapEntryListenerDelayMills);
                listeners.add(l);
                map.addEntryListener(l, true);
            }

            for (int count = 0; count < localMapEntryListenerCount; count++) {
                EntryListenerImpl l = new EntryListenerImpl(localMapEntryListenerDelayMills);
                listeners.add(l);

                map.addLocalEntryListener(l);
            }

            int v = 0;
            for (int k = 0; k < keyCount; k++) {
                map.put(k, values[v]);
                v = (v + 1 == values.length ? 0 : v + 1);
            }
        }
        log.info("===WARMUP===");
    }

    private String makeString(int length) {
        Random random = new Random();

        StringBuilder sb = new StringBuilder();
        for (int k = 0; k < length; k++) {
            char c = alphabet.charAt(random.nextInt(alphabet.length()));
            sb.append(c);
        }

        return sb.toString();
    }

    @Run
    public void run() {
        ThreadSpawner spawner = new ThreadSpawner();
        for (int k = 0; k < threadCount; k++) {
            spawner.spawn(new Worker());
        }
        spawner.awaitCompletion();
    }

    @Teardown
    public void globalTearDown() throws Exception {
        for (int i = 0; i < maxMaps; i++) {
            IMap map = targetInstance.getMap(basename + i);
            map.destroy();
        }
    }

    @Performance
    public long getOperationCount() {
        return 1;
    }


    @Verify
    public void verify() throws Exception {
        for(Object o : listeners){
            log.info(o.toString());
        }

        for (int i = 0; i < maxMaps; i++) {
            IMap map = targetInstance.getMap(basename + i);
            log.info(map.toString()+" sz="+map.size());
        }
    }


    private class Worker implements Runnable {
        private final Random random = new Random();
        int mapIdx, key;

        public void run() {

            while (!testContext.isStopped()) {

                if(randomDistributionUniform){
                    mapIdx = random.nextInt(maxMaps);
                    key = random.nextInt(keyCount);
                }else{
                    mapIdx = mapsZipfian.nextInt();
                    key = kesyZipfian.nextInt();

                }

                IMap map = targetInstance.getMap(basename + mapIdx);

                double chance = random.nextDouble();
                if (chance < writeProb) {

                    Object value = values[random.nextInt(values.length)];

                    chance = random.nextDouble();
                    if (chance < writeUsingPutProb) {
                        map.put(key, value);
                    }
                    else if(chance < writeUsingPutIfAbsent + writeUsingPutProb ){
                        map.putIfAbsent(key, value);
                    }
                    else if ( chance <  writeUsingPutExpireProb + writeUsingPutIfAbsent + writeUsingPutProb) {
                        int expire = random.nextInt(maxExpireMillis) + minExpireMillis;
                        map.put(key, value, expire, TimeUnit.MILLISECONDS);
                    }
                    else if ( chance < lockAndMod + writeUsingPutExpireProb + writeUsingPutIfAbsent + writeUsingPutProb) {

                        map.lock(key);
                        try{
                            value = map.get(key);
                            if(value==null){
                                value = values[random.nextInt(values.length)];
                            }
                            map.put(key, value);
                        }finally {
                            map.unlock(key);
                        }
                    }
                    else{
                        log.info("DID NOT ADD UP to (1) "+writeUsingPutExpireProb + writeUsingPutIfAbsent + writeUsingPutProb);
                    }

                }else if(chance < getProb + writeProb){
                    map.get(key);
                }
                else if(chance < clearProb + getProb + writeProb){
                    map.clear();
                }
                else if(chance < replaceProb + clearProb + getProb + writeProb){
                    Object value = values[random.nextInt(values.length)];
                    map.replace(key, value);
                }
                else if(chance < removeProb + replaceProb + clearProb + getProb + writeProb){
                    map.remove(key);
                }
                else if(chance < exicuteOnProb + removeProb + replaceProb + clearProb + getProb + writeProb){
                    map.executeOnKey(key, new EntryProcessorImpl(entryProcessorDelayMills));
                }
                else{
                    log.info("DID NOT ADD UP");
                }
            }
        }
    }



    public static class EntryProcessorImpl implements  EntryProcessor {

        public int entryProcessorDelayMills =0;

        public EntryProcessorImpl(int entryProcessorDelayNs){
            this.entryProcessorDelayMills = entryProcessorDelayNs;
        }

        public Object process(Map.Entry entry) {
            entry.setValue("modded Value");
            Utils.sleepMillis(entryProcessorDelayMills);
            return entry.getValue();
        }

        @Override
        public EntryBackupProcessor getBackupProcessor() {
            Utils.sleepMillis(entryProcessorDelayMills);
            return null;
        }

        @Override
        public String toString() {
            return "EntryProcessorImpl{" +
                    "entryProcessorDelayMills=" + entryProcessorDelayMills +
                    '}';
        }
    }


    public class MigrationnListenerImpl implements MigrationListener {
        public final AtomicInteger startedCount = new AtomicInteger();
        public final AtomicInteger completedCount = new AtomicInteger();
        public final AtomicInteger failedCount = new AtomicInteger();

        @Override
        public void migrationStarted(MigrationEvent migrationEvent) {
            Utils.sleepMillis(migrationListenerDelayMills);
            startedCount.incrementAndGet();
        }

        @Override
        public void migrationCompleted(MigrationEvent migrationEvent) {
            Utils.sleepMillis(migrationListenerDelayMills);
            completedCount.incrementAndGet();
        }

        @Override
        public void migrationFailed(MigrationEvent migrationEvent) {
            Utils.sleepMillis(migrationListenerDelayMills);
            failedCount.incrementAndGet();
        }

        @Override
        public String toString() {
            return "MigrationnListenerImpl{" +
                    "startedCount=" + startedCount +
                    ", completedCount=" + completedCount +
                    ", failedCount=" + failedCount +
                    '}';
        }
    }


    public class MembershipListenerImpl implements MembershipListener {
        public final AtomicInteger addCount = new AtomicInteger();
        public final AtomicInteger removeCount = new AtomicInteger();
        public final AtomicInteger updateCount = new AtomicInteger();

        @Override
        public void memberAdded(MembershipEvent membershipEvent) {
            Utils.sleepMillis(membershipListenerDelayMills);
            addCount.incrementAndGet();
        }

        @Override
        public void memberRemoved(MembershipEvent membershipEvent) {
            Utils.sleepMillis(membershipListenerDelayMills);
            removeCount.incrementAndGet();
        }

        @Override
        public void memberAttributeChanged(MemberAttributeEvent memberAttributeEvent) {
            Utils.sleepMillis(membershipListenerDelayMills);
            updateCount.incrementAndGet();
        }

        @Override
        public String toString() {
            return "MembershipListenerImpl{" +
                    "addCount=" + addCount +
                    ", removeCount=" + removeCount +
                    ", updateCount=" + updateCount +
                    '}';
        }
    }

    public class LifecycleListenerImpl implements LifecycleListener {
        public final AtomicInteger count = new AtomicInteger();

        @Override
        public void stateChanged(LifecycleEvent lifecycleEvent) {
            Utils.sleepMillis(lifecycleListenerDelayMills);
            count.incrementAndGet();
        }

        @Override
        public String toString() {
            return "LifecycleListenerImpl{" +
                    "count=" + count +
                    '}';
        }
    }


    public class DistributedObjectListenerImpl implements DistributedObjectListener {
        public final AtomicInteger addCount = new AtomicInteger();
        public final AtomicInteger removeCount = new AtomicInteger();

        @Override
        public void distributedObjectCreated(DistributedObjectEvent distributedObjectEvent) {
            Utils.sleepMillis(distributedObjectListenerDelayMills);
            addCount.incrementAndGet();
        }

        @Override
        public void distributedObjectDestroyed(DistributedObjectEvent distributedObjectEvent) {
            Utils.sleepMillis(distributedObjectListenerDelayMills);
            removeCount.incrementAndGet();
        }

        @Override
        public String toString() {
            return "DistributedObjectListenerImpl{" +
                    "addCount=" + addCount +
                    ", removeCount=" + removeCount +
                    '}';
        }
    }

    public class EntryListenerImpl implements EntryListener<Object, Object> {

        public final AtomicInteger addCount = new AtomicInteger();
        public final AtomicInteger removeCount = new AtomicInteger();
        public final AtomicInteger updateCount = new AtomicInteger();
        public final AtomicInteger evictCount = new AtomicInteger();
        private final int delay;

        public EntryListenerImpl(int delayNs) {
            this.delay = delayNs;
        }

        @Override
        public void entryAdded(EntryEvent<Object, Object> objectObjectEntryEvent) {
            Utils.sleepMillis(delay);
            addCount.incrementAndGet();
        }

        @Override
        public void entryRemoved(EntryEvent<Object, Object> objectObjectEntryEvent) {
            Utils.sleepMillis(delay);
            removeCount.incrementAndGet();
        }

        @Override
        public void entryUpdated(EntryEvent<Object, Object> objectObjectEntryEvent) {
            Utils.sleepMillis(delay);
            updateCount.incrementAndGet();
        }

        @Override
        public void entryEvicted(EntryEvent<Object, Object> objectObjectEntryEvent) {
            Utils.sleepMillis(delay);
            evictCount.incrementAndGet();
        }

        @Override
        public String toString() {
            return "EntryCounter{" +
                    "addCount=" + addCount +
                    ", removeCount=" + removeCount +
                    ", updateCount=" + updateCount +
                    ", evictCount=" + evictCount +
                    '}';
        }
    }

    public static void main(String[] args) throws Throwable {

        new TestRunner(new MapUsageStressTest()).run();
    }
}
