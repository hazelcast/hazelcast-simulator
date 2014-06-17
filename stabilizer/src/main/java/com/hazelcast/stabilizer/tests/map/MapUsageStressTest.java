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

import static org.junit.Assert.assertEquals;

public class MapUsageStressTest {

    private final static ILogger log = Logger.getLogger(MapUsageStressTest.class);
    private final static String alphabet = "abcdefghijklmnopqrstuvwxyz1234567890";

    private String basename = this.getClass().getName();
    public int threadCount = 3;
    public int valueLength = 100;
    public int keyCount = 1000;
    public int valueCount = 1000;
    public int maxMaps = 1;

    public boolean randomDistributionUniform=true;

    //check these add up to 1
    public double writeProb = 0.4;
    public double getProb = 0.2;
    public double removeProb = 0.2;
    public double deleteProb = 0.2;
    //

    //check these add up to 1   (writeProb is split up into sub styles)
    public double writeUsingPutProb = 0.25;
    public double writeUsingPutIfAbsent = 0.15;
    public double writeUsingPutExpireProb = 0.5;
    public double replaceProb = 0.1;
    //

    public int minExpireMillis = 200;
    public int maxExpireMillis = 8000;

    public int localMapEntryListenerCount = 1;
    public int mapEntryListenerCount = 1;

    private final AtomicInteger localAddCount = new AtomicInteger(0);
    private final AtomicInteger localRemoveCount = new AtomicInteger(0);
    private final AtomicInteger localUpdateCount = new AtomicInteger(0);
    private final AtomicInteger localEvictCount = new AtomicInteger(0);


    private String[] values;
    private TestContext testContext;
    private HazelcastInstance targetInstance;

    private Map<String, EntryListenerImpl> listeners = new HashMap();

    ScrambledZipfianGenerator mapsZipfian = new ScrambledZipfianGenerator(maxMaps);
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

        for (int i = 0; i < maxMaps; i++) {
            String name = basename + i;
            IMap map = targetInstance.getMap(name);

            for (int count = 0; count < mapEntryListenerCount; count++) {
                EntryListenerImpl l = new EntryListenerImpl();
                listeners.put(name, l);
                map.addEntryListener(l, true);
            }

            for (int count = 0; count < localMapEntryListenerCount; count++) {
                EntryListenerImpl l = new EntryListenerImpl();
                listeners.put(name+"local", l);
                map.addLocalEntryListener(l);
            }

            int v = 0;
            for (int k = 0; k < keyCount; k++) {
                map.put(k, values[v]);
                localAddCount.getAndIncrement();
                v = (v + 1 == values.length ? 0 : v + 1);
            }
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
        for (int i = 0; i < maxMaps; i++) {
            IMap map = targetInstance.getMap(basename + i);
            map.destroy();
        }
    }

    @Performance
    public long getOperationCount() {
        return 1;
    }


    @Verify(global = false)
    public void verify() throws Exception {

        Thread.sleep(10000);

        /*
        for (int i = 0; i < maxMaps; i++) {
            String name = basename + i;
            IMap map = targetInstance.getMap(name);
            EntryListenerImpl e = listeners.get(name);

            log.info(e.toString());


            int expectedMapSz = e.addCount.get() - (e.evictCount.get() + e.removeCount.get());
            assertEquals(expectedMapSz, map.size());
        }
        */

        for (int i = 0; i < localMapEntryListenerCount; i++) {
            String name = basename + i;
            EntryListenerImpl e = listeners.get(name+"local");

            while(true){


                System.out.println("add = "+localAddCount.get() +" "+ e.addCount.get());
                System.out.println("update = "+localUpdateCount.get() +" "+ e.updateCount.get());

                System.out.println("remove = "+localRemoveCount.get() +" "+ e.removeCount.get());
                System.out.println("evict = "+localEvictCount.get() +" "+ e.evictCount.get());


                Thread.sleep(3000);
                //assertEquals(localAddCount.get(), e.addCount.get());
                //assertEquals(localUpdateCount.get(), e.updateCount.get());
            }
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
                    else if ( chance <  writeUsingPutExpireProb + writeUsingPutIfAbsent + writeUsingPutProb) {
                        int expire = random.nextInt(maxExpireMillis) + minExpireMillis;

                        map.lock(key);
                        try{
                            if(map.containsKey(key)){
                                localUpdateCount.getAndIncrement();
                            }else {
                                localAddCount.getAndIncrement();
                            }
                            map.put(key, value, expire, TimeUnit.MILLISECONDS);
                        }
                        finally {
                            map.unlock(key);
                        }
                    }
                    else if(chance < replaceProb + writeUsingPutExpireProb + writeUsingPutIfAbsent + writeUsingPutProb){

                        Object orig = map.get(key);
                        if ( orig !=null && map.replace(key, orig, value) ){
                            localUpdateCount.getAndIncrement();
                        }
                    }


                }else if(chance < getProb + writeProb){
                    map.get(key);
                }
                else if(chance < removeProb + getProb + writeProb){
                    Object o = map.remove(key);
                    if(o != null){
                        localRemoveCount.getAndIncrement();
                    }
                }
                else if (chance < deleteProb + removeProb + getProb + writeProb ){

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

        public final AtomicInteger addCount = new AtomicInteger();
        public final AtomicInteger removeCount = new AtomicInteger();
        public final AtomicInteger updateCount = new AtomicInteger();
        public final AtomicInteger evictCount = new AtomicInteger();

        public EntryListenerImpl( ) {

        }

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
