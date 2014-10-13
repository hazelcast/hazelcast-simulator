package com.hazelcast.stabilizer.tests.icache;

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

import javax.cache.Cache;
import javax.cache.CacheException;
import javax.cache.CacheManager;
import javax.cache.spi.CachingProvider;
import java.io.Serializable;
import java.util.Random;

import static junit.framework.Assert.assertEquals;

/**
 * In This tests we intentionally creating destroying closing and using, cache managers and there caches
 * this type of cache usage well out side normal usage however we did find 2 bugs with this test
 */
public class MangleICacheTest {

    private final static ILogger log = Logger.getLogger(MangleICacheTest.class);

    public int threadCount = 3;
    public int maxCaches = 100;

    public double createCacheManager=0.1;
    public double cacheManagerClose=0.1;
    public double cacheManagerdestroy=0.1;
    public double cachingProviderClose=0.1;

    public double createCacheProb=0.1;
    public double destroyCacheProb=0.1;
    public double putCacheProb=0.3;
    public double closeCacheProb=0.1;

    private TestContext testContext;
    private HazelcastInstance targetInstance;
    private String basename;

    @Setup
    public void setup(TestContext testContext) throws Exception {
        this.testContext = testContext;
        targetInstance = testContext.getTargetInstance();
        basename = testContext.getTestId();
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
        private final Random random = new Random();
        private final CacheConfig config = new CacheConfig();
        private final Counter counter = new Counter();
        private CacheManager cacheManager=null;

        public void run() {
            config.setName(basename);
            createCacheManager();

            while (!testContext.isStopped()) {

                int cacheNumber = random.nextInt(maxCaches);
                double chance = random.nextDouble();
                if((chance -= createCacheManager) < 0){
                    try{
                        createCacheManager();
                        counter.createCacheManager++;

                    } catch (CacheException e) {
                        counter.createCacheManagerException++;

                    }
                }
                else if((chance -= cacheManagerClose) < 0){
                    try{
                        cacheManager.close();
                        counter.cacheManagerClose++;

                    } catch (CacheException e) {
                        counter.cacheManagerCloseException++;

                    }
                }
                else if((chance -= cacheManagerdestroy) < 0){
                    try{
                        cacheManager.destroyCache(basename + cacheNumber);
                        counter.cacheManagerdestroy++;

                    } catch (CacheException e) {
                        counter.cacheManagerdestroyException++;

                    } catch (IllegalStateException e) {
                        counter.cacheManagerdestroyException++;

                    }
                }
                else if((chance -= cachingProviderClose) < 0){
                    try{
                        CachingProvider provider = cacheManager.getCachingProvider();
                        if(provider!=null){
                            provider.close();
                            counter.cachingProviderClose++;
                        }
                    } catch (CacheException e) {
                        counter.cachingProviderCloseException++;

                    }
                }
                else if ((chance -= createCacheProb) < 0) {
                    try {
                        cacheManager.createCache(basename + cacheNumber, config);
                        counter.create++;

                    } catch (CacheException e) {
                        counter.createException++;

                    } catch (IllegalStateException e) {
                        counter.createException++;

                    }
                }
                else if ((chance -= putCacheProb) < 0) {
                    Cache cache=getAcache(cacheNumber);

                    try{
                        if(cache!=null){
                            cache.put(random.nextInt(), random.nextInt());
                            counter.put++;
                        }
                    } catch (CacheException e){
                        counter.getPutException++;

                    } catch (IllegalStateException e){
                        counter.getPutException++;

                    }
                }
                else if ((chance -= closeCacheProb) < 0){
                    Cache cache=getAcache(cacheNumber);
                    try{
                       if(cache!=null){
                            cache.close();
                            counter.cacheClose++;
                        }
                    } catch (CacheException e){
                        counter.cacheCloseException++;

                    } catch (IllegalStateException e){
                        counter.cacheCloseException++;

                    }
                }
                else if ((chance -= destroyCacheProb) < 0) {
                    try{
                        cacheManager.destroyCache(basename + cacheNumber);
                        counter.destroy++;

                    } catch (CacheException e){
                        counter.destroyException++;

                    } catch (IllegalStateException e){
                        counter.destroyException++;

                    }
                }
            }
            targetInstance.getList(basename).add(counter);
        }

        public void createCacheManager(){

            if (TestUtils.isMemberNode(targetInstance)) {
                HazelcastServerCachingProvider hcp = new HazelcastServerCachingProvider();
                cacheManager = new HazelcastServerCacheManager(
                        hcp, targetInstance, hcp.getDefaultURI(), hcp.getDefaultClassLoader(), null);
            } else {
                HazelcastClientCachingProvider hcp = new HazelcastClientCachingProvider();
                cacheManager = new HazelcastClientCacheManager(
                        hcp, targetInstance, hcp.getDefaultURI(), hcp.getDefaultClassLoader(), null);
            }
        }

        public Cache getAcache(int cacheNumber){
            try{
                Cache cache = cacheManager.getCache(basename + cacheNumber);
                counter.getCache++;
                return cache;

            } catch (CacheException e){
                counter.getCacheException++;

            } catch (IllegalStateException e){
                counter.getCacheException++;

            }
            return null;
        }
    }

    @Verify(global = true)
    public void verify() throws Exception {
        IList<Counter> counters = targetInstance.getList(basename);
        Counter total = new Counter();
        for(Counter c : counters){
            total.add(c);
        }
        log.info(basename + ": " + total + " from " + counters.size() + " worker threads");
    }

    static class Counter implements Serializable {

        public long createCacheManager = 0;
        public long createCacheManagerException = 0;

        public long getCache = 0;
        public long put = 0;
        public long create = 0;
        public long destroy = 0;
        public long cacheClose = 0;

        public long getCacheException = 0;
        public long getPutException = 0;
        public long createException = 0;
        public long destroyException = 0;
        public long cacheCloseException = 0;

        public long cacheManagerClose=0;
        public long cacheManagerdestroy=0;
        public long cachingProviderClose=0;

        public long cacheManagerCloseException=0;
        public long cacheManagerdestroyException=0;
        public long cachingProviderCloseException=0;

        public void add(Counter c) {

            createCacheManager += c.createCacheManager;
            createCacheManagerException += c.createCacheManagerException;

            getCache += c.getCache;
            put += c.put;
            create += c.create;
            destroy += c.destroy;
            cacheClose += c.cacheClose;

            getCacheException += c.getCacheException;
            getPutException += c.getPutException;
            createException += c.createException;
            destroyException += c.destroyException;
            cacheCloseException += c.cacheCloseException;

            cacheManagerClose += c.cacheManagerClose;
            cacheManagerdestroy += c.cacheManagerdestroy;
            cachingProviderClose += c.cachingProviderClose;
            cacheManagerCloseException += c.cacheManagerCloseException;
            cacheManagerdestroyException += c.cacheManagerdestroyException;
            cachingProviderCloseException += c.cachingProviderCloseException;
        }

        public String toString() {
            return "Counter{" +
                    "createCacheManager=" + createCacheManager +
                    ", createCacheManagerException=" + createCacheManagerException +
                    ", getCache=" + getCache +
                    ", put=" + put +
                    ", create=" + create +
                    ", destroy=" + destroy +
                    ", cacheClose=" + cacheClose +
                    ", getCacheException=" + getCacheException +
                    ", getPutException=" + getPutException +
                    ", createException=" + createException +
                    ", destroyException=" + destroyException +
                    ", cacheCloseException=" + cacheCloseException +
                    ", cacheManagerClose=" + cacheManagerClose +
                    ", cacheManagerdestroy=" + cacheManagerdestroy +
                    ", cachingProviderClose=" + cachingProviderClose +
                    ", cacheManagerCloseException=" + cacheManagerCloseException +
                    ", cacheManagerdestroyException=" + cacheManagerdestroyException +
                    ", cachingProviderCloseException=" + cachingProviderCloseException +
                    '}';
        }
    }
}