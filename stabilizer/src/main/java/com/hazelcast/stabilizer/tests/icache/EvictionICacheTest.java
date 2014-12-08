package com.hazelcast.stabilizer.tests.icache;

import com.hazelcast.cache.ICache;
import com.hazelcast.cache.impl.HazelcastServerCacheManager;
import com.hazelcast.cache.impl.HazelcastServerCachingProvider;
import com.hazelcast.client.cache.impl.HazelcastClientCacheManager;
import com.hazelcast.client.cache.impl.HazelcastClientCachingProvider;

import com.hazelcast.config.CacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IList;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.stabilizer.tests.TestContext;
import com.hazelcast.stabilizer.tests.annotations.Run;
import com.hazelcast.stabilizer.tests.annotations.Setup;
import com.hazelcast.stabilizer.tests.annotations.Verify;
import com.hazelcast.stabilizer.tests.utils.TestUtils;
import com.hazelcast.stabilizer.tests.utils.ThreadSpawner;
import com.hazelcast.stabilizer.worker.OperationSelector;

import javax.cache.CacheManager;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import static junit.framework.Assert.fail;

/*
* This test is expecting to work with an ICache which has a max-size policy and an eviction-policy
* defined.  the run body of the test simply puts random key value pairs to the ICache, and checks
* the size of the Icache has not grown above the defined max size + a configurable size margin.
* As the max-size policy is not a hard limit we use a configurable size margin in the verification
* of the cache size.  The test also logs the global max size of the ICache observed from all test
* participants, providing no assertion errors were throw.
* */
public class EvictionICacheTest {

    private final static ILogger log = Logger.getLogger(EvictionICacheTest.class);

    //number of threads each test participants will use to run the test
    public int threadCount=3;

    //number of bytes for the value/payload of a key
    public int valueSize=100;

    public double putProb=0.8;
    public double putAsyncProb=0.1;
    public double putAllProb=0.1;

    //used as the basename of the data structure
    public String basename;

    // As clients might be running this test they have no way of knowing the value
    // Default value is 271 or configure via "GroupProperties.PROP_PARTITION_COUNT" property
    public int partitionCount=271;


    private String id;
    private TestContext testContext;
    private HazelcastInstance targetInstance;
    private byte[] value;
    private ICache<Object, Object> cache;
    private int configuredMaxSize;
    private Map putAllMap = new HashMap();

    // Find balanced partition size if all entires are distributed perfectly
    private double balancedPartitionSize;

    // Square root of "balancedPartitionSize" gives values so close to our measured standard deviation values.
    private double approximatedStdDev;
    private int stdDevMultiplier;

    // Find estimated max partition size that any partition can reach at max
    private int estimatedMaxPartitionSize;

    // Find estimated max size (entry count) that cache can reach at max
    private int estimatedMaxSize = estimatedMaxPartitionSize * partitionCount;

    @Setup
    public void setup(TestContext testContex) throws Exception {
        this.testContext = testContex;
        targetInstance = testContext.getTargetInstance();
        id=testContex.getTestId();

        value = new byte[valueSize];
        Random random = new Random();
        random.nextBytes(value);

        CacheManager cacheManager;
        if (TestUtils.isMemberNode(targetInstance)) {
            HazelcastServerCachingProvider hcp = new HazelcastServerCachingProvider();
            cacheManager = new HazelcastServerCacheManager(
                    hcp, targetInstance, hcp.getDefaultURI(), hcp.getDefaultClassLoader(), null);
        } else {
            HazelcastClientCachingProvider hcp = new HazelcastClientCachingProvider();
            cacheManager = new HazelcastClientCacheManager(
                    hcp, targetInstance, hcp.getDefaultURI(), hcp.getDefaultClassLoader(), null);
        }
        cache = (ICache) cacheManager.getCache(basename);

        CacheConfig config = cache.getConfiguration(CacheConfig.class);
        log.info(id+": "+cache.getName()+" config="+config);

        configuredMaxSize = config.getMaxSizeConfig().getSize();

        for(int i=0; i< configuredMaxSize/2; i++){
            putAllMap.put(i, value);
        }

        balancedPartitionSize = (double) configuredMaxSize / (double) partitionCount;
        approximatedStdDev = Math.sqrt(balancedPartitionSize);
        stdDevMultiplier = configuredMaxSize <= 4000 ? 5 : 3;
        estimatedMaxPartitionSize = (int) (balancedPartitionSize + (approximatedStdDev * stdDevMultiplier));
        estimatedMaxSize = estimatedMaxPartitionSize * partitionCount;
    }

    @Run
    public void run() {
        ThreadSpawner spawner = new ThreadSpawner(testContext.getTestId());
        for(int i=0; i<threadCount; i++){
            spawner.spawn( new WorkerThread() );
        }
        spawner.awaitCompletion();
    }

    private class WorkerThread implements Runnable {
        Random random = new Random();
        int max=0;
        private OperationSelector<Operation> selector = new OperationSelector<Operation>();
        private Counter counter = new Counter();

        WorkerThread(){
            selector.addOperation(Operation.PUT, putProb)
                    .addOperation(Operation.PUT_ASYNC, putAsyncProb)
                    .addOperation(Operation.PUT_ALL, putAllProb);
        }

        @Override
        public void run() {
            while (!testContext.isStopped()) {

                int key = random.nextInt();

                switch (selector.select()) {
                    case PUT:
                        cache.put(key, value);
                        counter.put++;
                        break;

                    case PUT_ASYNC:
                        cache.putAsync(key, value);
                        counter.putAsync++;
                        break;

                    case PUT_ALL:
                        cache.putAll(putAllMap);
                        counter.putAll++;
                        break;
                }

                int size = cache.size();
                if(size > max){
                    max = size;
                }

                if(size > estimatedMaxSize){
                    fail(id + ": cache " + cache.getName() + " size=" + cache.size() + " configuredMaxSize=" + configuredMaxSize + " estimatedMaxSize=" + estimatedMaxSize);
                }
            }
            targetInstance.getList(basename+"max").add(max);
            targetInstance.getList(basename+"counter").add(counter);
        }

    }

    @Verify(global = true)
    public void globalVerify() throws Exception {
        IList<Integer> results = targetInstance.getList(basename+"max");
        int observedMaxSize=0;
        for (int m : results) {
            if(observedMaxSize < m){
                observedMaxSize = m;
            }
        }
        log.info(id + ": cache "+cache.getName()+" configuredMaxSize="+ configuredMaxSize +" observedMaxSize="+observedMaxSize+" estimatedMaxSize="+estimatedMaxSize);

        IList<Counter> counters = targetInstance.getList(basename+"counter");
        Counter total=new Counter();
        for (Counter c : counters) {
            total.add(c);
        }
        log.info(id + ": "+total);
    }

    public static class Counter implements Serializable {
        public int put=0;
        public int putAsync=0;
        public int putAll=0;

        public void add(Counter c){
            put+=c.put;
            putAsync+=c.putAsync;
            putAll+=c.putAll;
        }

        @Override
        public String toString() {
            return "Counter{" +
                    "put=" + put +
                    ", putAsync=" + putAsync +
                    ", putAll=" + putAll +
                    '}';
        }
    }

    static enum Operation {
        PUT,
        PUT_ASYNC,
        PUT_ALL,
    }
}