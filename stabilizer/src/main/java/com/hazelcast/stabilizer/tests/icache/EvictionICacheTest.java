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

import javax.cache.CacheManager;
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

    //the size of the cache can be larger than the defined max-size by this percentage before the test fails
    public double cacheSizeMargin=0.2;

    //used as the basename of the data structure
    public String basename;

    private String id;
    private TestContext testContext;
    private HazelcastInstance targetInstance;
    private byte[] value;
    private ICache<Object, Object> cache;
    private int maxSize;
    private int threshold;

    @Setup
    public void setup(TestContext testContex) throws Exception {
        this.testContext = testContex;
        targetInstance = testContext.getTargetInstance();
        id=testContex.getTestId();

        value = new byte[valueSize];
        Random random = new Random();
        random.nextBytes(value);

        //size 1000
        //size 271*15  + 15
        //30000

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

        maxSize = config.getMaxSizeConfig().getSize();
        threshold = (int) (maxSize * cacheSizeMargin) + maxSize;
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

        @Override
        public void run() {
            while (!testContext.isStopped()) {

                int key = random.nextInt();
                cache.put(key, value);

                int size = cache.size();
                if(size > max){
                    max = size;
                }

                if(size > threshold){
                    fail(id + ": cache " + cache.getName() + " size=" + cache.size() + " max=" + maxSize + " threshold=" + threshold);
                }
            }
            targetInstance.getList(basename+"max").add(max);
        }
    }

    @Verify(global = true)
    public void globalVerify() throws Exception {
        IList<Integer> results = targetInstance.getList(basename+"max");
        int max=0;
        for (int m : results) {
            if(max < m){
                max = m;
            }
        }
        log.info(id + ": cache "+cache.getName()+" max size ="+max);
    }
}