package com.hazelcast.stabilizer.tests.icache;

import com.hazelcast.cache.impl.HazelcastCacheManager;
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
import com.hazelcast.stabilizer.tests.annotations.Warmup;
import com.hazelcast.stabilizer.tests.icache.helpers.MyCacheEntryEventFilter;
import com.hazelcast.stabilizer.tests.icache.helpers.MyCacheEntryListener;
import com.hazelcast.stabilizer.tests.utils.TestUtils;
import com.hazelcast.stabilizer.tests.utils.ThreadSpawner;

import javax.cache.Cache;
import javax.cache.configuration.FactoryBuilder;
import javax.cache.configuration.MutableCacheEntryListenerConfiguration;
import java.io.Serializable;
import java.util.Random;

/**
 * In This test we concurrently add remove cache listeners while putting and getting from the cache
 * this test is out side of normal usage, however has found problems where put operations hang
 * **/
public class AddRemoveListenerICacheTest {

    private final static ILogger log = Logger.getLogger(AddRemoveListenerICacheTest.class);

    public int threadCount = 3;
    public int keyCount = 1000;
    public boolean syncEvents = true;

    public double register=0;
    public double deregister=0;
    public double put=0;
    public double get=0;

    private TestContext testContext;
    private HazelcastInstance targetInstance;
    private HazelcastCacheManager cacheManager;
    private String basename;

    private CacheConfig<Integer, Long> config = new CacheConfig<Integer, Long>();
    private Cache<Integer, Long> cache;
    private MyCacheEntryListener<Integer, Long> listener;
    private MyCacheEntryEventFilter<Integer, Long> filter;

    private MutableCacheEntryListenerConfiguration m;

    @Setup
    public void setup(TestContext textConTx) {
        testContext = textConTx;
        targetInstance = testContext.getTargetInstance();
        basename = testContext.getTestId();

        if (TestUtils.isMemberNode(targetInstance)) {
            HazelcastServerCachingProvider hcp = new HazelcastServerCachingProvider();
            cacheManager = new HazelcastServerCacheManager(hcp, targetInstance, hcp.getDefaultURI(), hcp.getDefaultClassLoader(), null);
        } else {
            HazelcastClientCachingProvider hcp = new HazelcastClientCachingProvider();
            cacheManager = new HazelcastClientCacheManager( hcp, targetInstance, hcp.getDefaultURI(), hcp.getDefaultClassLoader(), null);
        }

        config.setName(basename);
        cacheManager.createCache(basename, config);
    }

    @Warmup(global = false)
    public void warmup() {
        cache = cacheManager.getCache(basename);

        listener = new MyCacheEntryListener<Integer, Long>();
        filter = new MyCacheEntryEventFilter<Integer, Long>();

        m = new MutableCacheEntryListenerConfiguration<Integer, Long>(
                FactoryBuilder.factoryOf(listener),
                FactoryBuilder.factoryOf(filter),
                false, syncEvents);
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
        private Random random = new Random();
        private Counter counter = new Counter();

        public void run() {
            while (!testContext.isStopped()) {

                double chance = random.nextDouble();
                if((chance -= register) < 0){
                    try{
                        cache.registerCacheEntryListener(m);
                        counter.register++;
                    }catch(IllegalArgumentException e){
                        counter.registerIllegalArgException++;
                    }
                }
                else if((chance -= deregister) < 0){
                    cache.deregisterCacheEntryListener(m);
                    counter.deregister++;

                }else if((chance -= put) < 0){
                    cache.put(random.nextInt(keyCount), 1l);
                    counter.put++;
                }
                else if((chance -= get) < 0){
                    cache.get(random.nextInt(keyCount));
                    counter.put++;
                }
            }
            log.info(basename + ": "+counter);
            targetInstance.getList(basename).add(counter);
        }
    }

    @Verify(global = true)
    public void globalVerify() throws Exception {

        IList<Counter> results = targetInstance.getList(basename);
        Counter total = new Counter();
        for (Counter i : results) {
            total.add(i);
        }
        log.info(basename + ": " + total + " from " + results.size() + " worker Threads");
    }

    private static class Counter implements Serializable {
        public long put;
        public long get;
        public long register;
        public long registerIllegalArgException;
        public long deregister;


        public void add(Counter c) {
            put += c.put;
            get += c.get;
            register += c.register;
            registerIllegalArgException += c.registerIllegalArgException;
            deregister += c.deregister;
        }

        public String toString() {
            return "Counter{" +
                    "put=" + put +
                    ", get=" + get +
                    ", register=" + register +
                    ", registerIllegalArgException=" + registerIllegalArgException +
                    ", deregister=" + deregister +
                    '}';
        }
    }
}
