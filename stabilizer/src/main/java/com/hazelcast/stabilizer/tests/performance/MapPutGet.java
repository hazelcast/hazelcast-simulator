package com.hazelcast.stabilizer.tests.performance;

import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IList;
import com.hazelcast.core.IMap;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.stabilizer.tests.TestContext;
import com.hazelcast.stabilizer.tests.TestRunner;
import com.hazelcast.stabilizer.tests.annotations.Run;
import com.hazelcast.stabilizer.tests.annotations.Setup;
import com.hazelcast.stabilizer.tests.annotations.Verify;
import com.hazelcast.stabilizer.tests.annotations.Warmup;
import com.hazelcast.stabilizer.tests.utils.TestUtils;
import com.hazelcast.stabilizer.tests.utils.ThreadSpawner;
import org.HdrHistogram.IntHistogram;

import java.util.Random;

import static com.hazelcast.stabilizer.tests.utils.TestUtils.nextKeyOwnedBy;

/*
* Test performance of map put/get.  gives latency histogram and through put
* every member node in the test,  init's the map with keysPerNode number of key/value
* during initialization each member puts to a partition they own,  this cuts down on network traffic during
* test init.  In total there will be keysPerNode * memberCount keys in the map.  the value used is
* a byte array,  the test uses the same byte array to put as the value for every map entry, so
* cutting down the memory footprint of the test.
* the clients or members that run the test, do puts and get (at ratio putProb) from the key range 0 to totalKeys,  so
* their puts and gets can hit every member.  put / get latency and through put info is collected from every worker thread
* so there is no competition between worker threads.  this test also has a JIT warm up phase before the measurements are take
* */
public class MapPutGet {
    private final static ILogger log = Logger.getLogger(MapPutGet.class);

    public String basename = this.getClass().getName();
    public int threadCount = 3;
    public int valueLength = 1000;
    public int keysPerNode = 1000;
    public int memberCount = 1;
    public int jitWarmUpMs = 1000*30;
    public int durationMs = 1000*60;
    public double putProb = 0.5;

    private TestContext testContext;
    private HazelcastInstance targetInstance;
    private IMap map;
    private byte[] value;
    private int totalKeys;

    @Setup
    public void setup(TestContext testContex) throws Exception {
        testContext = testContex;
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
                map.put((int)key, value);
                key++;
            }
            totalKeys = keysPerNode * memberCount;
        }
    }

    @Run
    public void run() {
        ThreadSpawner spawner = new ThreadSpawner(testContext.getTestId());

        Worker[] workers = new Worker[threadCount];

        for(int i=0; i<threadCount; i++){
            workers[i] = new Worker();
            spawner.spawn(workers[i]);
        }
        spawner.awaitCompletion();

        for(int i=1; i<threadCount; i++){
            workers[0].putLatencyHisto.add(workers[i].putLatencyHisto);
            workers[0].getLatencyHisto.add(workers[i].getLatencyHisto);
        }

        targetInstance.getList(basename+"putHisto").add(workers[0].putLatencyHisto);
        targetInstance.getList(basename+"getHisto").add(workers[0].getLatencyHisto);
    }

    private class Worker implements Runnable {
        IntHistogram putLatencyHisto = new IntHistogram(1, 1000*30, 2);
        IntHistogram getLatencyHisto = new IntHistogram(1, 1000*30, 2);
        Random random = new Random();

        public void run() {
            test(jitWarmUpMs);
            putLatencyHisto.reset();
            getLatencyHisto.reset();
            test(durationMs);
        }

        private void test(long maxTime){
            long runStart = System.currentTimeMillis();
            long now;
            do{
                int key = random.nextInt(totalKeys);

                if(random.nextDouble() < putProb){
                    long start = System.currentTimeMillis();
                    map.put(key, value);
                    long stop = System.currentTimeMillis();
                    putLatencyHisto.recordValue(stop - start);
                }else{
                    long start = System.currentTimeMillis();
                    map.get(key);
                    long stop = System.currentTimeMillis();
                    getLatencyHisto.recordValue(stop - start);
                }

                now = System.currentTimeMillis();
            }while(now - runStart < maxTime);
        }
    }

    @Verify(global = true)
    public void verify() throws Exception {

        MapConfig mapConfig = targetInstance.getConfig().getMapConfig(basename);
        log.info(basename+": "+mapConfig);
        log.info(basename+": map size ="+map.size());

        IList<IntHistogram>  putHistos = targetInstance.getList(basename+"putHisto");
        IList<IntHistogram>  getHistos = targetInstance.getList(basename+"getHisto");

        IntHistogram putHisto = putHistos.get(0);
        IntHistogram getHisto = getHistos.get(0);

        for(int i=1; i<putHistos.size(); i++){
            putHisto.add(putHistos.get(i));
        }
        for(int i=1; i<getHistos.size(); i++){
            getHisto.add(getHistos.get(i));
        }

        log.info(basename + ": Put Latency Histogram");
        putHisto.outputPercentileDistribution(System.out, 1.0);
        double putsPerSec = putHisto.getTotalCount() / (durationMs/1000);

        log.info(basename + ": Get Latency Histogram");
        getHisto.outputPercentileDistribution(System.out, 1.0);
        double getPerSec = getHisto.getTotalCount() / (durationMs/1000);

        log.info(basename+": put/sec ="+putsPerSec);
        log.info(basename+": get/Sec ="+getPerSec);

        System.out.println(putHisto.getEstimatedFootprintInBytes());
    }

    public static void main(String[] args) throws Throwable {
        MapPutGet test = new MapPutGet();
        new TestRunner(test).run();
    }
}