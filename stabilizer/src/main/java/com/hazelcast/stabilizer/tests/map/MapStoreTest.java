package com.hazelcast.stabilizer.tests.map;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.stabilizer.tests.TestContext;
import com.hazelcast.stabilizer.tests.TestRunner;
import com.hazelcast.stabilizer.tests.annotations.Run;
import com.hazelcast.stabilizer.tests.annotations.Setup;
import com.hazelcast.stabilizer.tests.annotations.Verify;
import com.hazelcast.stabilizer.tests.map.helpers.MapStoreWithCounter;
import com.hazelcast.stabilizer.tests.utils.ThreadSpawner;

import java.util.Random;

import static junit.framework.Assert.assertEquals;

/**
 * Created by danny on 6/27/14.
 */
public class MapStoreTest {

    public String basename = this.getClass().getName();
    public int threadCount = 3;
    public int keyCount = 10;

    //check these add up to 1
    public double writeProb = 0.45;
    public double evictProb = 0.25;
    public double removeProb = 0.15;
    public double deleteProb = 0.15;
    public double destroyProb = 0.5;
    //

    //check these add up to 1   (writeProb is split up into sub styles)
    public double writeUsingPutProb = 0.5;
    public double writeUsingPutIfAbsent = 0.25;
    public double replaceProb = 0.25;
    //

    private TestContext testContext;
    private HazelcastInstance targetInstance;

    public MapStoreTest(){}

    @Setup
    public void setup(TestContext testContext) throws Exception {
        this.testContext = testContext;
        targetInstance = testContext.getTargetInstance();
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
        private final Random random = new Random();

        @Override
        public void run() {
            while (!testContext.isStopped()) {

                final int key = random.nextInt(keyCount);
                final IMap map = targetInstance.getMap(basename);

                double chance = random.nextDouble();
                if (chance < writeProb) {

                    final Object value = random.nextInt(keyCount);

                    chance = random.nextDouble();
                    if (chance < writeUsingPutProb) {
                        map.put(key, value);
                    }
                    else if(chance < writeUsingPutIfAbsent + writeUsingPutProb ){
                        //map.putIfAbsent(key, value);
                    }
                    else if(chance < replaceProb + writeUsingPutIfAbsent + writeUsingPutProb){
                        Object orig = map.get(key);
                        if ( orig !=null ){
                            //map.replace(key, orig, value);
                        }
                    }
                }else if(chance < evictProb + writeProb){
                    //map.evict(key);
                }
                else if(chance < removeProb + evictProb + writeProb){
                    //map.remove(key);
                }
                else if (chance < deleteProb + removeProb + evictProb + writeProb ){
                    //map.delete(key);
                }
                else if (chance < destroyProb + deleteProb + removeProb + evictProb + writeProb ){
                    //map.destroy();
                }
            }
        }
    }

    @Verify(global = false)
    public void verify() throws Exception {

        Thread.sleep(7000);

        System.out.println("verify "+basename+" !!");

        final IMap map = targetInstance.getMap(basename);
        MapStoreWithCounter mapStore = (MapStoreWithCounter) targetInstance.getConfig().getMapConfig(basename).getMapStoreConfig().getImplementation();

        System.out.println("map size       =" + map.size() );
        System.out.println("map local      =" + map.getAll(map.localKeySet()).entrySet() );
        System.out.println("map Store      =" + mapStore.store.entrySet() );

        //this is still wrong as some other node is putting to the keys you own.
        //how to do it 1) real DB,  TimeStamp on all events, and check the last ones
        /*
        for(Object k: map.localKeySet()){
            Object storeValue = mapStore.store.get(k);
            assertEquals( map.get(k), storeValue );
        }
        */
    }

    public static void main(String[] args) throws Throwable {
        new TestRunner(new MapStoreTest()).run();
    }

}