package com.hazelcast.stabilizer.tests.icache;

import com.hazelcast.cache.ICache;

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
import com.hazelcast.stabilizer.test.TestContext;
import com.hazelcast.stabilizer.test.annotations.Run;
import com.hazelcast.stabilizer.test.annotations.Setup;
import com.hazelcast.stabilizer.test.annotations.Verify;
import com.hazelcast.stabilizer.test.annotations.Warmup;
import com.hazelcast.stabilizer.tests.icache.helpers.MyCacheEntryEventFilter;
import com.hazelcast.stabilizer.tests.icache.helpers.MyCacheEntryListener;
import com.hazelcast.stabilizer.test.utils.ThreadSpawner;

import javax.cache.CacheException;
import javax.cache.configuration.CacheEntryListenerConfiguration;
import javax.cache.configuration.FactoryBuilder;
import javax.cache.configuration.MutableCacheEntryListenerConfiguration;
import javax.cache.expiry.CreatedExpiryPolicy;
import javax.cache.expiry.Duration;
import javax.cache.expiry.ExpiryPolicy;
import java.io.Serializable;
import java.util.Random;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.stabilizer.utils.CommonUtils.sleepSeconds;
import static com.hazelcast.stabilizer.tests.helpers.HazelcastTestUtils.isMemberNode;
import static junit.framework.Assert.assertEquals;

/*
* In this test was adding listeners to a cache, and recording the number of events the listeners recive
* and compare that to the number of events we should have generated using put / get operations ect
* we Verify that no unexpected events have been received
* */
public class ListenerICacheTest {

    private static final int PAUSE_FOR_LAST_EVENTS_SECONDS = 10;

    private static final ILogger log = Logger.getLogger(ListenerICacheTest.class);

    public int threadCount = 3;
    public int maxExpiryDurationMs = 500;
    public int keyCount = 1000;
    public boolean syncEvents = true;

    public double put = 0.5;
    public double putExpiry = 0.0;
    public double putAsyncExpiry = 0.0;
    public double getExpiry = 0.0;
    public double getAsyncExpiry = 0.0;
    public double remove = 0.1;
    public double replace = 0.1;

    private TestContext testContext;
    private HazelcastInstance targetInstance;
    private CacheManager cacheManager;
    private String basename;

    private CacheConfig<Integer, Long> config = new CacheConfig<Integer, Long>();
    private ICache<Integer, Long> cache;
    private MyCacheEntryListener<Integer, Long> listener;
    private MyCacheEntryEventFilter<Integer, Long> filter;

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
        try {
            cacheManager.createCache(basename, config);
        } catch (CacheException ignored) {
        }
    }

    @Warmup(global = false)
    public void warmup() {
        cache = (ICache) cacheManager.getCache(basename);

        listener = new MyCacheEntryListener<Integer, Long>();
        filter = new MyCacheEntryEventFilter<Integer, Long>();

        CacheEntryListenerConfiguration<Integer, Long> conf = new MutableCacheEntryListenerConfiguration<Integer, Long>(
                FactoryBuilder.factoryOf(listener),
                FactoryBuilder.factoryOf(filter),
                false, syncEvents);

        cache.registerCacheEntryListener(conf);
    }

    @Run
    public void run() {
        ThreadSpawner spawner = new ThreadSpawner(testContext.getTestId());
        for (int k = 0; k < threadCount; k++) {
            spawner.spawn(new Worker());
        }
        spawner.awaitCompletion();

        sleepSeconds(PAUSE_FOR_LAST_EVENTS_SECONDS);

        targetInstance.getList(basename+"listeners").add(listener);
    }

    private class Worker implements Runnable {
        private Random random = new Random();
        private Counter counter = new Counter();

        public void run() {
            while (!testContext.isStopped()) {

                int expiryDuration = random.nextInt(maxExpiryDurationMs);
                ExpiryPolicy expiryPolicy = new CreatedExpiryPolicy(new Duration(TimeUnit.MILLISECONDS, expiryDuration));

                int k = random.nextInt(keyCount);

                double chance = random.nextDouble();
                if ((chance -= put) < 0) {
                    cache.put(k, random.nextLong());
                    counter.put++;

                }
                else if ((chance -= putExpiry) < 0) {
                    cache.put(k, random.nextLong(), expiryPolicy);
                    counter.putExpiry++;

                } else if ((chance -= putAsyncExpiry) < 0) {
                    cache.putAsync(k, random.nextLong(), expiryPolicy);
                    counter.putAsyncExpiry++;

                } else if ((chance -= getExpiry) < 0) {
                    cache.get(k, expiryPolicy);
                    counter.getExpiry++;

                } else if ((chance -= getAsyncExpiry) < 0) {
                    Future<Long> f = cache.getAsync(k, expiryPolicy);
                    try {
                        f.get();
                        counter.getAsyncExpiry++;
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }

                } else if ((chance -= remove) < 0) {
                    if ( cache.remove(k) ){
                        counter.remove++;
                    }

                } else if ((chance -= replace) < 0) {
                    if ( cache.replace(k, random.nextLong()) ){
                        counter.replace++;
                    }

                }
            }
            targetInstance.getList(basename).add(counter);
        }
    }

    @Verify(global = false)
    public void verify() throws Exception {
        log.info(basename + ": listener " + listener);
        log.info(basename + ": filter " + filter);
    }

    @Verify(global = true)
    public void globalVerify() throws Exception {

        IList<Counter> results = targetInstance.getList(basename);
        Counter total = new Counter();
        for (Counter i : results) {
            total.add(i);
        }
        log.info(basename + ": " + total + " from " + results.size() + " worker Threads");

        IList<MyCacheEntryListener> listeners = targetInstance.getList(basename+"listeners");
        MyCacheEntryListener totalEvents = new MyCacheEntryListener();
        for(MyCacheEntryListener listener : listeners){
            totalEvents.add(listener);
        }
        log.info(basename + ": totalEvents " + totalEvents);

        assertEquals(basename + ": unExpected Events found ", 0, totalEvents.unExpected.get());
    }

    private static class Counter implements Serializable {
        public long put;
        public long putExpiry;
        public long putAsyncExpiry;
        public long getExpiry;
        public long getAsyncExpiry;
        public long remove;
        public long replace;

        public void add(Counter c) {
            put += c.put;
            putExpiry += c.putExpiry;
            putAsyncExpiry += c.putAsyncExpiry;
            getExpiry += c.getExpiry;
            getAsyncExpiry += c.getAsyncExpiry;
            remove += c.remove;
            replace += c.replace;
        }

        public String toString() {
            return "Counter{" +
                    "put=" + put +
                    ", putExpiry=" + putExpiry +
                    ", putAsyncExpiry=" + putAsyncExpiry +
                    ", getExpiry=" + getExpiry +
                    ", getAsyncExpiry=" + getAsyncExpiry +
                    ", remove=" + remove +
                    ", replace=" + replace +
                    '}';
        }
    }
}
