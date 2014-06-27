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
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.stabilizer.tests.TestContext;
import com.hazelcast.stabilizer.tests.TestRunner;
import com.hazelcast.stabilizer.tests.annotations.*;
import com.hazelcast.stabilizer.tests.map.helpers.Count;
import com.hazelcast.stabilizer.tests.map.helpers.EntryListenerImpl;
import com.hazelcast.stabilizer.tests.map.helpers.ScrambledZipfianGenerator;
import com.hazelcast.stabilizer.tests.utils.ThreadSpawner;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;

public class MapEntryListenerTest {

    private final static ILogger log = Logger.getLogger(MapEntryListenerTest.class);
    private final static String alphabet = "abcdefghijklmnopqrstuvwxyz1234567890";

    private String basename = this.getClass().getName();
    public int threadCount = 3;
    public int valueLength = 100;
    public int keyCount = 1000;
    public int valueCount = 1000;

    public boolean randomDistributionUniform=false;

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

    private int mapEntryListenerCount = 1;

    private Count count = new Count();

    private String[] values;
    private TestContext testContext;
    private HazelcastInstance targetInstance;

    private Map<String, EntryListenerImpl> listeners = new HashMap();

    public static final AtomicBoolean addResult = new AtomicBoolean(true);


    private ScrambledZipfianGenerator kesyZipfian = new ScrambledZipfianGenerator(keyCount);

    @Setup
    public void setup(TestContext testContext) throws Exception {
        this.testContext = testContext;
        targetInstance = testContext.getTargetInstance();

        values = new String[valueCount];
        for (int k = 0; k < values.length; k++) {
            values[k] = makeString(valueLength);
        }

        IMap map = targetInstance.getMap(basename);

        for (int i = 0; i < mapEntryListenerCount; i++) {
            EntryListenerImpl l = new EntryListenerImpl();
            listeners.put(basename, l);
            map.addEntryListener(l, true);
        }
    }

    @Warmup(global = true)
    public void globalWarmup() {

        ILock lock = targetInstance.getLock(basename+"lock");

        if(lock.tryLock()){

            IMap map = targetInstance.getMap(basename);

            int v = 0;
            for (int k = 0; k < keyCount; k++) {
                map.put(k, values[v]);
                count.localAddCount.getAndIncrement();
                v = (v + 1 == values.length ? 0 : v + 1);
            }
            System.out.println("init map with "+keyCount+" items");

            //so we are assuming that the node who makes the global warmup is not active in the test
            //so you put his results in hear as this is all the effect he has on the test
            IList results = targetInstance.getList(basename+"results");
            results.add(count);
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
        System.out.println("Running with threads="+threadCount);

        ThreadSpawner spawner = new ThreadSpawner( testContext.getTestId() );
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
        return count.total();
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
                                count.localUpdateCount.getAndIncrement();
                            }else {
                                count.localAddCount.getAndIncrement();
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
                                count.localAddCount.getAndIncrement();
                            }
                        }finally {
                            map.unlock(key);
                        }
                    }
                    else if(chance < replaceProb + writeUsingPutIfAbsent + writeUsingPutProb){
                        Object orig = map.get(key);
                        if ( orig !=null && map.replace(key, orig, value) ){
                            count.localReplaceCount.getAndIncrement();
                        }
                    }
                }else if(chance < evictProb + writeProb){
                    map.lock(key);
                    try{
                        if(map.containsKey(key)){
                            count.localEvictCount.getAndIncrement();
                        }
                        map.evict(key);
                    }finally {
                        map.unlock(key);
                    }
                }
                else if(chance < removeProb + evictProb + writeProb){
                    Object o = map.remove(key);
                    if(o != null){
                        count.localRemoveCount.getAndIncrement();
                    }
                }
                else if (chance < deleteProb + removeProb + evictProb + writeProb ){
                    map.lock(key);
                    try{
                        if(map.containsKey(key)){
                            count.localRemoveCount.getAndIncrement();
                        }
                        map.delete(key);
                    }finally {
                        map.unlock(key);
                    }
                }
            }

            IList resultListners = targetInstance.getList(basename+"listeners");
            IList results = targetInstance.getList(basename+"results");
            if(addResult.compareAndSet(true, false)){

                try {
                    Thread.sleep(6000);
                } catch (InterruptedException e) { e.printStackTrace(); }

                results.add(count);
                resultListners.addAll(listeners.values());

                System.out.println("actions for This = "+count);
            }

        }
    }


    @Verify(global = true)
    public void golbalVerify() throws Exception {

        ILock lock = targetInstance.getLock(basename+"Lock2");
        if(lock.tryLock()){
            System.out.println("Global verify ");

            IList<Count> counts = targetInstance.getList(basename+"results");
            for(Count c : counts){
                System.out.println(c);
            }

            IList<EntryListenerImpl> resultListners = targetInstance.getList(basename+"listeners");
            for(int i=0; i<resultListners.size()-1; i++){
                EntryListenerImpl a = resultListners.get(i);
                EntryListenerImpl b = resultListners.get(i+1);

                assertEquals("not same amount of event in all listeners", a, b);
            }
        }
    }


    @Verify(global = false)
    public void verify() throws Exception {
        System.out.println("verify ");

        IList<Count> counts = targetInstance.getList(basename+"results");
        Count total = new Count();
        for(Count c : counts){
            total.add(c);
        }

        IMap map = targetInstance.getMap(basename);
        EntryListenerImpl e = listeners.get(basename);

        long addedTotal = total.localAddCount.get() + total.localReplaceCount.get();
        long replaceTrickCount = e.addCount.get() - total.localAddCount.get();
        long expectedMapSz = e.addCount.get()  - (total.localReplaceCount.get() + e.evictCount.get() + e.removeCount.get());

        System.out.println("add = "+addedTotal +" "+ e.addCount.get());
        System.out.println("update = "+total.localUpdateCount.get() +" "+ e.updateCount.get());
        System.out.println("remove = "+total.localRemoveCount.get() +" "+ e.removeCount.get());
        System.out.println("evict = "+total.localEvictCount.get() +" "+ e.evictCount.get());
        System.out.println("replaced = " + total.localReplaceCount.get() + " " + replaceTrickCount);
        System.out.println("mapSZ = "+ map.size() + " " + expectedMapSz );

        assertEquals(" Add Events ",      addedTotal,                     e.addCount.get());
        assertEquals(" Update Events ",   total.localUpdateCount.get(),   e.updateCount.get());
        assertEquals(" Remove Events ",   total.localRemoveCount.get(),   e.removeCount.get());
        assertEquals(" Evict Events ",    total.localEvictCount.get(),    e.evictCount.get());

        assertEquals(" Add Events caused By Replace ", total.localReplaceCount.get(), replaceTrickCount);

        assertEquals(" MapSZ ", expectedMapSz, map.size());
    }

    public static void main(String[] args) throws Throwable {
        new TestRunner(new MapEntryListenerTest()).run();
    }
}
