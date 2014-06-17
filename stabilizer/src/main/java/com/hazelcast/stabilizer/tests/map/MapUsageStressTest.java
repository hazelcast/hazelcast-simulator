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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;

public class MapUsageStressTest {

    private final static ILogger log = Logger.getLogger(MapUsageStressTest.class);
    private final static String alphabet = "abcdefghijklmnopqrstuvwxyz1234567890";

    private String basename = this.getClass().getName();
    public int threadCount = 3;
    public int valueLength = 100;
    public int keyCount = 1000;
    public int valueCount = 1000;

    public boolean randomDistributionUniform=true;

    //check these add up to 1
    public double writeProb = 0.4;
    public double evictProb = 0.2;
    public double removeProb = 0.2;
    public double deleteProb = 0.2;
    //

    //check these add up to 1   (writeProb is split up into sub styles)
    public double writeUsingPutProb = 0.5;
    public double writeUsingPutIfAbsent = 0.25;
    public double replaceProb = 0.25;
    //

    public int mapEntryListenerCount = 1;

    private final AtomicLong localAddCount = new AtomicLong(0);
    private final AtomicLong localRemoveCount = new AtomicLong(0);
    private final AtomicLong localUpdateCount = new AtomicLong(0);
    private final AtomicLong localEvictCount = new AtomicLong(0);


    private String[] values;
    private TestContext testContext;
    private HazelcastInstance targetInstance;

    private Map<String, EntryListenerImpl> listeners = new HashMap();

    ScrambledZipfianGenerator kesyZipfian = new ScrambledZipfianGenerator(keyCount);

    @Setup
    public void setup(TestContext testContext) throws Exception {
        this.testContext = testContext;
        targetInstance = testContext.getTargetInstance();
    }

    @Warmup(global = true)
    public void warmup() {
        values = new String[valueCount];

        for (int k = 0; k < values.length; k++) {
            values[k] = makeString(valueLength);
        }

        IMap map = targetInstance.getMap(basename);

        for (int count = 0; count < mapEntryListenerCount; count++) {
            EntryListenerImpl l = new EntryListenerImpl();
            listeners.put(basename, l);
            map.addEntryListener(l, true);
        }

        int v = 0;
        for (int k = 0; k < keyCount; k++) {
            map.put(k, values[v]);
            localAddCount.getAndIncrement();
            v = (v + 1 == values.length ? 0 : v + 1);
        }
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

    @Teardown(global = true)
    public void tearDown() throws Exception {
        IMap map = targetInstance.getMap(basename);
        map.destroy();
    }

    @Performance
    public long getOperationCount() {
        return 1;
    }

    @Verify(global = false)
    public void verify() throws Exception {
        Thread.sleep(5000);

        IMap map = targetInstance.getMap(basename);
        EntryListenerImpl e = listeners.get(basename);

        System.out.println("add = "+localAddCount.get() +" "+ e.addCount.get());
        System.out.println("update = "+localUpdateCount.get() +" "+ e.updateCount.get());
        System.out.println("remove = "+localRemoveCount.get() +" "+ e.removeCount.get());
        System.out.println("evict = "+localEvictCount.get() +" "+ e.evictCount.get());

        long expectedMapSz = e.addCount.get() - (e.evictCount.get() + e.removeCount.get());
        assertEquals(expectedMapSz, map.size());

        //assertEquals(localAddCount.get(), e.addCount.get());
        //assertEquals(localUpdateCount.get(), e.updateCount.get());
    }

    private class Worker implements Runnable {
        private final Random random = new Random();
        int key;

        public void run() {

            while (!testContext.isStopped()) {

                if(randomDistributionUniform){
                    key = random.nextInt(keyCount);
                }else{
                    key = kesyZipfian.nextInt();
                }

                IMap map = targetInstance.getMap(basename);

                double chance = random.nextDouble();
                if (chance < writeProb) {

                    Object value = values[random.nextInt(values.length)];

                    chance = random.nextDouble();
                    if (chance < writeUsingPutProb) {
                        map.lock(key);
                        try{
                            if(map.containsKey(key)){
                                localUpdateCount.getAndIncrement();
                            }else {
                                localAddCount.getAndIncrement();
                            }
                            map.put(key, value);
                        }finally {
                            map.unlock(key);
                        }
                    }
                    else if(chance < writeUsingPutIfAbsent + writeUsingPutProb ){
                        map.lock(key);
                        try{
                            if(map.putIfAbsent(key, value) == null ){
                                localAddCount.getAndIncrement();
                            }
                        }finally {
                            map.unlock(key);
                        }
                    }
                    else if(chance < replaceProb + writeUsingPutIfAbsent + writeUsingPutProb){
                        Object orig = map.get(key);
                        if ( orig !=null && map.replace(key, orig, value) ){
                            localAddCount.getAndIncrement();
                        }
                    }
                }else if(chance < evictProb + writeProb){
                    map.lock(key);
                    try{
                        if(map.containsKey(key)){
                            localEvictCount.getAndIncrement();
                        }
                        map.evict(key);
                    }finally {
                        map.unlock(key);
                    }
                }
                else if(chance < removeProb + evictProb + writeProb){
                    Object o = map.remove(key);
                    if(o != null){
                        localRemoveCount.getAndIncrement();
                    }
                }
                else if (chance < deleteProb + removeProb + evictProb + writeProb ){
                    map.lock(key);
                    try{
                        if(map.containsKey(key)){
                            localRemoveCount.getAndIncrement();
                        }
                        map.delete(key);
                    }finally {
                        map.unlock(key);
                    }
                }
            }
        }
    }


    public class EntryListenerImpl implements EntryListener<Object, Object> {

        public final AtomicLong addCount = new AtomicLong();
        public final AtomicLong removeCount = new AtomicLong();
        public final AtomicLong updateCount = new AtomicLong();
        public final AtomicLong evictCount = new AtomicLong();

        public EntryListenerImpl( ) { }

        @Override
        public void entryAdded(EntryEvent<Object, Object> objectObjectEntryEvent) {
            addCount.incrementAndGet();
        }

        @Override
        public void entryRemoved(EntryEvent<Object, Object> objectObjectEntryEvent) {
            removeCount.incrementAndGet();
        }

        @Override
        public void entryUpdated(EntryEvent<Object, Object> objectObjectEntryEvent) {
            updateCount.incrementAndGet();
        }

        @Override
        public void entryEvicted(EntryEvent<Object, Object> objectObjectEntryEvent) {
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
