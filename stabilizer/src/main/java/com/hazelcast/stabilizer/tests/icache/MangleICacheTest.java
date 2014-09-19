package com.hazelcast.stabilizer.tests.icache;

import com.hazelcast.cache.impl.HazelcastServerCacheManager;
import com.hazelcast.cache.impl.HazelcastServerCachingProvider;
import com.hazelcast.client.cache.HazelcastClientCacheManager;
import com.hazelcast.client.cache.HazelcastClientCachingProvider;
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
    public double putCacheProb=0.4;

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
                        log.severe(basename+": createCacheManager "+e, e);
                        counter.createCacheManagerException++;

                    } catch (Exception e) {
                        log.severe(basename+": createCacheManager ERROR "+e, e);
                        counter.generalException++;
                    }
                }
                else if((chance -= cacheManagerClose) < 0){
                    try{
                        cacheManager.close();
                        counter.cacheManagerClose++;

                    } catch (CacheException e) {
                        log.severe(basename+": cacheManagerClose "+e, e);
                        counter.cacheManagerCloseException++;

                    } catch (Exception e) {
                        log.severe(basename+": cacheManagerClose ERROR "+e, e);
                        counter.generalException++;
                    }
                }
                else if((chance -= cacheManagerdestroy) < 0){
                    try{
                        cacheManager.destroyCache(basename + cacheNumber);
                        counter.cacheManagerdestroy++;

                    } catch (CacheException e) {
                        log.severe(basename+": cacheManagerdestroy "+e, e);
                        counter.cacheManagerdestroyException++;

                    } catch (IllegalStateException e) {
                        log.severe(basename+": cacheManagerdestroy "+e, e);
                        counter.cacheManagerdestroyException++;

                    } catch (Exception e) {
                        log.severe(basename+": cacheManagerdestroy ERROR "+e, e);
                        counter.generalException++;
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
                        log.severe(basename+": cachingProviderClose "+e, e);
                        counter.cachingProviderCloseException++;

                    } catch (Exception e) {
                        log.severe(basename+": cachingProviderClose ERROR "+e, e);
                        counter.generalException++;
                    }
                }
                else if ((chance -= createCacheProb) < 0) {
                    try {
                        cacheManager.createCache(basename + cacheNumber, config);
                        counter.create++;

                    } catch (CacheException e) {
                        log.severe(basename+": create "+e, e);
                        counter.createException++;

                    } catch (IllegalStateException e) {
                        log.severe(basename+": create "+e, e);
                        counter.createException++;

                    } catch (Exception e) {
                        log.severe(basename+": create ERROR "+e, e);
                        counter.generalException++;
                    }
                }
                else if ((chance -= putCacheProb) < 0) {
                    Cache cache=null;

                    try{
                        cache = cacheManager.getCache(basename + cacheNumber);
                        counter.getCache++;

                    } catch (CacheException e){
                        log.severe(basename+": getCache "+e, e);
                        counter.getCacheException++;

                    } catch (IllegalStateException e){
                        log.severe(basename+": getCache "+e, e);
                        counter.getCacheException++;

                    } catch (Exception e){
                        log.severe(basename+": getCache ERROR "+e, e);
                        counter.generalException++;
                    }

                    try{
                        if(cache!=null){
                            cache.put(random.nextInt(), random.nextInt());
                            counter.put++;
                        }
                    } catch (CacheException e){
                        log.severe(basename+": putCache "+e, e);
                        counter.getPutException++;

                    } catch (IllegalStateException e){
                        log.severe(basename+": putCache "+e, e);
                        counter.getPutException++;

                    } catch (Exception e){
                        log.severe(basename+": putCache ERROR "+e, e);
                        counter.generalException++;
                    }

                }
                else if ((chance -= destroyCacheProb) < 0) {
                    try{
                        cacheManager.destroyCache(basename + cacheNumber);
                        counter.destroy++;

                    } catch (CacheException e){
                        log.severe(basename+": destroy "+e, e);
                        counter.destroyException++;

                    } catch (IllegalStateException e){
                        log.severe(basename+": destroy "+e, e);
                        counter.destroyException++;

                    } catch (Exception e){
                        log.severe(basename+": destroy ERROR "+e, e);
                        counter.generalException++;
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
    }

    @Verify(global = true)
    public void verify() throws Exception {
        IList<Counter> counters = targetInstance.getList(basename);
        Counter total = new Counter();
        for(Counter c : counters){
            total.add(c);
        }
        log.info(basename + ": collected results from " + counters.size() + " worker threads");
        log.info(basename + ": " + total);

        assertEquals(basename + ": " + total.generalException + " Unexpected Exceptions were thrown during the test ", 0, total.generalException);
    }

    static class Counter implements Serializable {

        public long createCacheManager = 0;
        public long createCacheManagerException = 0;

        public long getCache = 0;
        public long put = 0;
        public long create = 0;
        public long destroy = 0;

        public long getCacheException = 0;
        public long getPutException = 0;
        public long createException = 0;
        public long destroyException = 0;

        public long cacheManagerClose=0;
        public long cacheManagerdestroy=0;
        public long cachingProviderClose=0;

        public long cacheManagerCloseException=0;
        public long cacheManagerdestroyException=0;
        public long cachingProviderCloseException=0;

        public long generalException=0;

        public void add(Counter c) {

            createCacheManager += c.createCacheManager;
            createCacheManagerException += c.createCacheManagerException;

            getCache += c.getCache;
            put += c.put;
            create += c.create;
            destroy += c.destroy;

            getCacheException += c.getCacheException;
            getPutException += c.getPutException;
            createException += c.createException;
            destroyException += c.destroyException;

            cacheManagerClose += c.cacheManagerClose;
            cacheManagerdestroy += c.cacheManagerdestroy;
            cachingProviderClose += c.cachingProviderClose;
            cacheManagerCloseException += c.cacheManagerCloseException;
            cacheManagerdestroyException += c.cacheManagerdestroyException;
            cachingProviderCloseException += c.cachingProviderCloseException;

            generalException += c.generalException;
        }

        @Override
        public String toString() {
            return "Counter{" +
                    "createCacheManager=" + createCacheManager +
                    ", createCacheManagerException=" + createCacheManagerException +
                    ", getCache=" + getCache +
                    ", put=" + put +
                    ", create=" + create +
                    ", destroy=" + destroy +
                    ", getCacheException=" + getCacheException +
                    ", getPutException=" + getPutException +
                    ", createException=" + createException +
                    ", destroyException=" + destroyException +
                    ", cacheManagerClose=" + cacheManagerClose +
                    ", cacheManagerdestroy=" + cacheManagerdestroy +
                    ", cachingProviderClose=" + cachingProviderClose +
                    ", cacheManagerCloseException=" + cacheManagerCloseException +
                    ", cacheManagerdestroyException=" + cacheManagerdestroyException +
                    ", cachingProviderCloseException=" + cachingProviderCloseException +
                    ", generalException=" + generalException +
                    '}';
        }
    }
}