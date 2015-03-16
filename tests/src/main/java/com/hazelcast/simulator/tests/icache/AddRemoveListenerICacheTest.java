package com.hazelcast.simulator.tests.icache;

import javax.cache.CacheManager;
import com.hazelcast.cache.impl.HazelcastServerCacheManager;
import com.hazelcast.cache.impl.HazelcastServerCachingProvider;
import com.hazelcast.client.cache.impl.HazelcastClientCacheManager;
import com.hazelcast.client.cache.impl.HazelcastClientCachingProvider;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IList;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.test.annotations.Run;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Verify;
import com.hazelcast.simulator.test.annotations.Warmup;
import com.hazelcast.simulator.tests.icache.helpers.MyCacheEntryEventFilter;
import com.hazelcast.simulator.tests.icache.helpers.MyCacheEntryListener;
import com.hazelcast.simulator.test.utils.ThreadSpawner;

import javax.cache.Cache;
import javax.cache.configuration.FactoryBuilder;
import javax.cache.configuration.MutableCacheEntryListenerConfiguration;
import java.io.Serializable;
import java.util.Random;

import static com.hazelcast.simulator.tests.helpers.HazelcastTestUtils.isMemberNode;

/**
 * In This test we concurrently add remove cache listeners while putting and getting from the cache
 * this test is out side of normal usage, however has found problems where put operations hang
 * this type of test could uncover memory leaks in the process of adding and removing listeners
 * The max size of the cache used in this test is keyCount int key/value pairs,
 * **/
public class AddRemoveListenerICacheTest {

    private static final ILogger log = Logger.getLogger(AddRemoveListenerICacheTest.class);

    public int threadCount = 3;
    public int keyCount = 1000;
    public boolean syncEvents = true;

    public double register=0;
    public double deregister=0;
    public double put=0;
    public double get=0;

    private TestContext testContext;
    private HazelcastInstance targetInstance;
    private CacheManager cacheManager;
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

        if (isMemberNode(targetInstance)) {
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
