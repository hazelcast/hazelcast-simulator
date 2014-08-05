package com.hazelcast.stabilizer.tests.map;

import com.hazelcast.config.MaxSizeConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IList;
import com.hazelcast.core.IMap;
import com.hazelcast.spi.exception.DistributedObjectDestroyedException;
import com.hazelcast.stabilizer.tests.TestContext;
import com.hazelcast.stabilizer.tests.annotations.Run;
import com.hazelcast.stabilizer.tests.annotations.Setup;
import com.hazelcast.stabilizer.tests.annotations.Verify;
import com.hazelcast.stabilizer.tests.map.helpers.MapOpperationsCount;
import com.hazelcast.stabilizer.tests.utils.ThreadSpawner;

import java.util.Random;
import java.util.concurrent.TimeUnit;


public class MapMaxSizeTest {

    public String basename = this.getClass().getName();
    public int threadCount = 3;
    public int keyCount = Integer.MAX_VALUE;

    //check these add up to 1
    public double writeProb = 0.5;
    public double getProb = 0.5;
    //

    //check these add up to 1   (writeProb is split up into sub styles)
    public double writeUsingPutProb = 0.6;
    public double writeUsingPutAsyncProb = 0.2;
    public double writeUsingPutTTLProb = 0.2;
    //

    public int maxTTLExpireyMs = 3000;
    public int minTTLExpireyMs = 100;

    private TestContext testContext;
    private HazelcastInstance targetInstance;

    public MapMaxSizeTest(){}

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
        private MapOpperationsCount count = new MapOpperationsCount();
        private final Random random = new Random();

        @Override
        public void run() {
            while (!testContext.isStopped()) {
                try{
                    final int key = random.nextInt(keyCount);
                    final IMap map = targetInstance.getMap(basename);

                    double chance = random.nextDouble();
                    if ( (chance -= writeProb) < 0 ) {

                        final Object value = random.nextInt();

                        chance = random.nextDouble();
                        if ( (chance -= writeUsingPutProb) < 0) {
                            map.put(key, value);
                            count.putCount.incrementAndGet();
                        }
                        else if ( (chance -= writeUsingPutAsyncProb) < 0 ) {
                            map.putAsync(key, value);
                            count.putAsyncCount.incrementAndGet();
                        }
                        else if ( (chance -= writeUsingPutTTLProb ) < 0 ) {
                            long delayMs = minTTLExpireyMs + random.nextInt(maxTTLExpireyMs);
                            map.put(key, delayMs, delayMs, TimeUnit.MILLISECONDS);
                            count.putTTLCount.incrementAndGet();
                        }

                    }else if( (chance -= getProb) < 0 ){
                        map.get(key);
                        count.getCount.incrementAndGet();
                    }

                }catch(DistributedObjectDestroyedException e){}
            }
            IList results = targetInstance.getList(basename+"report");
            results.add(count);
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

        final IMap map = targetInstance.getMap(basename);
        MaxSizeConfig maxSizeConfig = targetInstance.getConfig().getMapConfig(basename).getMaxSizeConfig();

        System.out.println(basename+": Map size = "+map.size()+" Max size="+maxSizeConfig.getSize());
    }

    @Verify(global = false)
    public void verify() throws Exception {
        try{
            Thread.sleep(maxTTLExpireyMs*2);

            final IMap map = targetInstance.getMap(basename);

            System.out.println(basename+ ": map size  =" + map.size() );
            System.out.println(basename+ ": map local =" + map.getAll(map.localKeySet()).entrySet() );

        }catch(UnsupportedOperationException e){}
    }

}