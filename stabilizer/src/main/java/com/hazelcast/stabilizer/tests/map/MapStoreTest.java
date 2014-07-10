package com.hazelcast.stabilizer.tests.map;

import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IList;
import com.hazelcast.core.IMap;
import com.hazelcast.stabilizer.tests.TestContext;
import com.hazelcast.stabilizer.tests.TestRunner;
import com.hazelcast.stabilizer.tests.annotations.Run;
import com.hazelcast.stabilizer.tests.annotations.Setup;
import com.hazelcast.stabilizer.tests.annotations.Verify;
import com.hazelcast.stabilizer.tests.map.helpers.Count;
import com.hazelcast.stabilizer.tests.map.helpers.MapOpperationsCount;
import com.hazelcast.stabilizer.tests.map.helpers.MapStoreWithCounter;
import com.hazelcast.stabilizer.tests.utils.ThreadSpawner;

import java.util.Random;
import java.util.concurrent.TimeUnit;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNull;

public class MapStoreTest {

    public String basename = this.getClass().getName();
    public int threadCount = 3;
    public int keyCount = 10;

    private int putTTlKeyDomain;
    private int putTTlKeyRange;


    //check these add up to 1
    public double writeProb = 0.4;
    public double getProb = 0.2;
    public double getAsyncProb = 0.15;
    public double deleteProb = 0.2;
    public double destroyProb = 0.0;
    //

    //check these add up to 1   (writeProb is split up into sub styles)
    public double writeUsingPutProb = 0.4;
    public double writeUsingPutTTLProb = 0.3;
    public double writeUsingPutIfAbsent = 0.15;
    public double writeUsingReplaceProb = 0.15;
    //

    private int maxExpireySeconds = 3;


    private TestContext testContext;
    private HazelcastInstance targetInstance;
    private MapOpperationsCount count = new MapOpperationsCount();

    public MapStoreTest(){}

    @Setup
    public void setup(TestContext testContext) throws Exception {
        this.testContext = testContext;
        targetInstance = testContext.getTargetInstance();
        putTTlKeyDomain = keyCount;
        putTTlKeyRange = keyCount;
    }

    @Run
    public void run() {
        ThreadSpawner spawner = new ThreadSpawner(testContext.getTestId());
        for (int k = 0; k < threadCount; k++) {
            spawner.spawn(new Worker());
        }
        spawner.awaitCompletion();

        IList results = targetInstance.getList(basename+"report");
        results.add(count);
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
                        count.putCount.incrementAndGet();
                    }
                    else if (chance < writeUsingPutTTLProb + writeUsingPutProb) {
                        long delay = 1 + random.nextInt(maxExpireySeconds);
                        int k =  putTTlKeyDomain + random.nextInt(putTTlKeyRange);
                        map.putTransient(k, delay, delay, TimeUnit.SECONDS);
                        count.putTransientCount.incrementAndGet();
                    }
                    else if(chance < writeUsingPutIfAbsent + writeUsingPutTTLProb + writeUsingPutProb ){
                        map.putIfAbsent(key, value);
                        count.putIfAbsentCount.incrementAndGet();
                    }
                    else if(chance < writeUsingReplaceProb + writeUsingPutIfAbsent + writeUsingPutTTLProb + writeUsingPutProb){
                        Object orig = map.get(key);
                        if ( orig !=null ){
                            map.replace(key, orig, value);
                            count.replaceCount.incrementAndGet();
                        }
                    }

                }else if(chance < getProb + writeProb){
                    map.get(key);
                    count.getCount.incrementAndGet();
                }
                else if(chance < getAsyncProb + getProb + writeProb){
                    map.getAsync(key);
                    count.getAsyncCount.incrementAndGet();
                }
                else if (chance < deleteProb + getAsyncProb + getProb + writeProb ){
                    map.delete(key);
                    count.deleteCount.incrementAndGet();
                }
                else if (chance < destroyProb + deleteProb + getAsyncProb + getProb + writeProb ){
                    map.destroy();
                    count.destroyCount.incrementAndGet();
                }
            }
        }
    }

    @Verify(global = true)
    public void globalVerify() throws Exception {

        IList<MapOpperationsCount> results = targetInstance.getList(basename+"report");
        MapOpperationsCount total = new MapOpperationsCount();
        for(MapOpperationsCount i : results){
            total.add(i);
        }
        System.out.println(basename+": "+total+" total of "+results.size());
    }

    @Verify(global = false)
    public void verify() throws Exception {

        try{
            MapStoreConfig mapStoreConfig = targetInstance.getConfig().getMapConfig(basename).getMapStoreConfig();
            final int writeDelaySeconds = mapStoreConfig.getWriteDelaySeconds();
            Thread.sleep( (writeDelaySeconds*2 + maxExpireySeconds + 1) * 1000 );

            final MapStoreWithCounter mapStore = (MapStoreWithCounter) mapStoreConfig.getImplementation();
            final IMap map = targetInstance.getMap(basename);

            System.out.println(basename+ ": map size  =" + map.size() );
            System.out.println(basename+ ": map local =" + map.getAll(map.localKeySet()).entrySet() );
            System.out.println(basename+ ": map Store =" + mapStore.store.entrySet() );

            System.out.println(basename+ ": "+ mapStore);

            for(Object k: map.localKeySet()){
                assertEquals( map.get(k), mapStore.store.get(k) );
            }

            assertEquals("local key set of each member should be equal to the local instance mapStore size "
                    , map.localKeySet().size(), mapStore.store.size());

            for(int k = putTTlKeyDomain; k < putTTlKeyDomain + putTTlKeyRange; k++){
                assertNull("TTL key should not be in the map", map.get(k) );
            }

        }catch(UnsupportedOperationException e){}
    }

    public static void main(String[] args) throws Throwable {
        new TestRunner(new MapStoreTest()).run();
    }
}