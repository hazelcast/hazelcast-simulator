package com.hazelcast.stabilizer.tests.icache;

import com.hazelcast.cache.ICache;
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
import com.hazelcast.stabilizer.tests.utils.TestUtils;
import com.hazelcast.stabilizer.tests.utils.ThreadSpawner;

import javax.cache.CacheException;
import javax.cache.configuration.FactoryBuilder;
import javax.cache.configuration.MutableCacheEntryListenerConfiguration;
import javax.cache.event.CacheEntryCreatedListener;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryEventFilter;
import javax.cache.event.CacheEntryListenerException;
import javax.cache.event.EventType;
import javax.cache.expiry.CreatedExpiryPolicy;
import javax.cache.expiry.Duration;
import javax.cache.expiry.ExpiryPolicy;
import java.io.Serializable;
import java.util.Random;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.stabilizer.tests.utils.TestUtils.sleepMs;

public class ListenerICache {

    private final static ILogger log = Logger.getLogger(ListenerICache.class);

    public int threadCount = 3;
    public int maxExpiryDurationMs = 500;
    public int keyCount = 1000;


    public double put = 0.5;
    public double putExpiry = 0.0;
    public double putAsyncExpiry = 0.0;
    public double getExpiry = 0.0;
    public double getAsyncExpiry = 0.0;
    public double remove = 0.1;
    public double replace = 0.1;


    private TestContext testContext;
    private HazelcastInstance targetInstance;
    private HazelcastCacheManager cacheManager;
    private String basename;

    private CacheConfig<Integer, Long> config = new CacheConfig();
    private ICache<Integer, Long> cache;
    private MyCacheEntryListener<Integer, Long> listener;
    private MyCacheEntryEventFilter<Integer, Long> filter;

    private int paussForLastEvents=1000 * 10;

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
        config.setTypes(Integer.class, Long.class);
        try{
            cacheManager.createCache(basename, config);
        }catch(CacheException e){
        }
    }

    @Warmup(global = false)
    public void warmup() {
        cache = cacheManager.getCache(basename, config.getKeyType(), config.getValueType());

        listener = new MyCacheEntryListener();
        filter = new MyCacheEntryEventFilter();

        cache.registerCacheEntryListener(
                new MutableCacheEntryListenerConfiguration<Integer, Long> (FactoryBuilder.factoryOf(listener),
                        FactoryBuilder.factoryOf(filter)
                        ,false,false));
    }

    @Run
    public void run() {
        ThreadSpawner spawner = new ThreadSpawner(testContext.getTestId());
        for (int k = 0; k < threadCount; k++) {
            spawner.spawn(new Worker());
        }
        spawner.awaitCompletion();

        sleepMs(paussForLastEvents);

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
                    Long value = cache.get(k, expiryPolicy);
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
    public void Verify() throws Exception {
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

        final ICache<Integer, Long> cache = cacheManager.getCache(basename, Integer.class, Long.class);
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

    public static class MyCacheEntryListener<K, V> implements CacheEntryCreatedListener<K, V>, Serializable {

        public AtomicLong created = new AtomicLong();
        public AtomicLong updated = new AtomicLong();
        public AtomicLong removed = new AtomicLong();
        public AtomicLong expired = new AtomicLong();
        public AtomicLong unExpected = new AtomicLong();

        public void onCreated(Iterable<CacheEntryEvent<? extends K, ? extends V>> events) throws CacheEntryListenerException {

            for (CacheEntryEvent<? extends K, ? extends V> event : events) {

                switch (event.getEventType()){
                    case CREATED:
                        created.incrementAndGet();
                        break;
                    case UPDATED:
                        updated.incrementAndGet();
                        break;
                    case REMOVED:
                        removed.incrementAndGet();
                        break;
                    case EXPIRED:
                        expired.incrementAndGet();
                        break;
                    default:
                        unExpected.incrementAndGet();
                        break;
                }
            }

        }

        public String toString() {
            return "MyCacheEntryListener{" +
                    "created=" + created +
                    ", updated=" + updated +
                    ", removed=" + removed +
                    ", expired=" + expired +
                    ", unExpected=" + unExpected +
                    '}';
        }
    }

    public static class MyCacheEntryEventFilter<K, V> implements CacheEntryEventFilter<K, V>, Serializable {

        public EventType eventFilter = null;//EventType.CREATED;
        public final AtomicLong filtered = new AtomicLong();

        public boolean evaluate(CacheEntryEvent<? extends K, ? extends V> event)  throws CacheEntryListenerException {

            if (eventFilter!=null && event.getEventType() == eventFilter){
                filtered.incrementAndGet();
                return false;
            }
            return true;
        }

        @Override
        public String toString() {
            return "MyCacheEntryEventFilter{" +
                    "eventFilter=" + eventFilter +
                    ", filtered=" + filtered +
                    '}';
        }
    }
}
