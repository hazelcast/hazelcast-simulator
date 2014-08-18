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
import com.hazelcast.stabilizer.tests.map.helpers.OppCounterMapMaxSizeTest;
import com.hazelcast.stabilizer.tests.utils.ThreadSpawner;

import java.util.Random;
import java.util.concurrent.TimeUnit;

import static junit.framework.Assert.assertNull;
import static junit.framework.Assert.assertTrue;


public class MapMaxSizeTest {

    public String basename = this.getClass().getName();
    public int threadCount = 3;
    public int keyCount = Integer.MAX_VALUE;

    //check these add up to 1
    public double writeProb = 0.5;
    public double getProb = 0.4;
    public double checkSizeProb = 0.1;
    //

    //check these add up to 1   (writeProb is split up into sub styles)
    public double writeUsingPutProb = 0.8;
    public double writeUsingPutAsyncProb = 0.2;
    //


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
        private OppCounterMapMaxSizeTest count = new OppCounterMapMaxSizeTest();
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
                            count.put++;
                        }
                        else if ( (chance -= writeUsingPutAsyncProb) < 0 ) {
                            map.putAsync(key, value);
                            count.putAsync++;
                        }

                    }else if( (chance -= getProb) < 0 ){
                        map.get(key);
                        count.get++;
                    }
                    else if( (chance -= checkSizeProb) <0){
                        int clusterSize = targetInstance.getCluster().getMembers().size();
                        assertTrue("Map Over max Size",  map.size() < clusterSize * 1000 );
                        count.verified++;
                    }

                }catch(Exception e){
                    System.out.println(basename+": "+e);
                    e.printStackTrace();
                }
            }
            targetInstance.getList(basename+"report").add(count);
        }
    }

    @Verify(global = true)
    public void globalVerify() throws Exception {

        IList<OppCounterMapMaxSizeTest> results = targetInstance.getList(basename+"report");
        OppCounterMapMaxSizeTest total = new OppCounterMapMaxSizeTest();
        for(OppCounterMapMaxSizeTest i : results){
            total.add(i);
        }
        System.out.println(basename+": "+total+" from "+results.size()+" workers");

        IMap map = targetInstance.getMap(basename);
        System.out.println(basename+": Map size = "+map.size());

        int clusterSize = targetInstance.getCluster().getMembers().size();
        assertTrue("Map Over max Size",  map.size() < clusterSize * 1000 );
    }

    @Verify(global = false)
    public void verify() throws Exception {
        try{
            MaxSizeConfig maxSizeConfig = targetInstance.getConfig().getMapConfig(basename).getMaxSizeConfig();
            System.out.println(basename+": "+maxSizeConfig);
        }catch (Exception e){}
    }

}