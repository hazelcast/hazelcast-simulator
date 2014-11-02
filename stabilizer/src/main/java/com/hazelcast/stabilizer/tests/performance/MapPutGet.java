package com.hazelcast.stabilizer.tests.performance;

import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.stabilizer.tests.TestContext;
import com.hazelcast.stabilizer.tests.annotations.Run;
import com.hazelcast.stabilizer.tests.annotations.Setup;
import com.hazelcast.stabilizer.tests.annotations.Warmup;
import com.hazelcast.stabilizer.tests.utils.TestUtils;
import com.hazelcast.stabilizer.tests.utils.ThreadSpawner;

import java.util.Random;

import static com.hazelcast.stabilizer.tests.utils.TestUtils.nextKeyOwnedBy;

public class MapPutGet {
    private final static ILogger log = Logger.getLogger(MapPutGet.class);

    public String basename = this.getClass().getName();
    public int valueLength = 1000;
    public int keysPerNode = 1000;
    public int memberCount = 3;

    private TestContext testContext;
    private HazelcastInstance targetInstance;
    private IMap map;
    private byte[] value;
    private int totalKeys;

    @Setup
    public void setup(TestContext testContext) throws Exception {
        this.testContext = testContext;
        targetInstance = testContext.getTargetInstance();
        map = targetInstance.getMap(basename);
        value = new byte[valueLength];

        Random random = new Random();
        random.nextBytes(value);

        if(TestUtils.isMemberNode(targetInstance)){
            TestUtils.waitClusterSize(log, targetInstance, memberCount);
            TestUtils.warmupPartitions(log, targetInstance);

            long key=0;
            for(int i=0; i<keysPerNode; i++){
                key = nextKeyOwnedBy(key, targetInstance);
                map.put(key, value);
                key++;
            }
            totalKeys = keysPerNode * memberCount;
        }
    }

    @Warmup(global = false)
    public void warmup() throws Exception {

        MapConfig mapConfig = targetInstance.getConfig().getMapConfig(basename);
        log.info(basename+": "+mapConfig);
    }

    @Run
    public void run() {
        ThreadSpawner spawner = new ThreadSpawner(testContext.getTestId());
        spawner.spawn(new Worker());
        spawner.awaitCompletion();
    }

    private class Worker implements Runnable {

        long puts=0;
        double putAvgLatency=0.0;
        double putMinLatency=0.0;
        double putMaxLatency=0.0;

        long gets=0;
        boolean put=true;

        int[] allKeys = new int[totalKeys];
        int[] latencyHisto = new int[500];

        public Worker(){
            for(int i=0; i<totalKeys; i++){
                allKeys[i]=i;
            }
            shuffleArray(allKeys);
        }

        public void run() {
            int keyIdx=0;
            while (!testContext.isStopped()) {

                int key = allKeys[keyIdx];
                if(put){
                    long start = System.currentTimeMillis();
                    map.put(key, value);
                    long stop = System.currentTimeMillis();
                    long latency = stop - start;
                    putAvgLatency = (latency - putAvgLatency) / ++puts;

                    if(latency < putMinLatency){
                        putMinLatency = latency;
                    }
                    else if(latency > putMaxLatency){
                        putMaxLatency = latency;
                    }

                    int histoIdx=0;
                    while(latency <  ){
                        histoIdx++;
                    }

                }else{
                    map.get(key);
                    gets++;
                }
                put=!put;
                keyIdx = (++keyIdx == totalKeys) ? 0 : keyIdx;
            }
        }

        void shuffleArray(int[] ar){

            Random rnd = new Random();
            for (int i = ar.length - 1; i > 0; i--) {
                int index = rnd.nextInt(i + 1);
                // Simple swap
                int temp = ar[index];
                ar[index] = ar[i];
                ar[i] = temp;
            }
        }
    }

}

