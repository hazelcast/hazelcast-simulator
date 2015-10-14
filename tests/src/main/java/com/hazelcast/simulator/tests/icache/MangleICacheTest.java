package com.hazelcast.simulator.tests.icache;

import com.hazelcast.config.CacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IList;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.test.annotations.RunWithWorker;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Verify;
import com.hazelcast.simulator.tests.icache.helpers.CacheUtils;
import com.hazelcast.simulator.tests.icache.helpers.ICacheOperationCounter;
import com.hazelcast.simulator.worker.selector.OperationSelectorBuilder;
import com.hazelcast.simulator.worker.tasks.AbstractWorker;

import javax.cache.Cache;
import javax.cache.CacheException;
import javax.cache.CacheManager;
import javax.cache.spi.CachingProvider;

/**
 * In this tests we are intentionally creating, destroying, closing and using cache managers and their caches.
 *
 * This type of cache usage is well outside normal usage, however we found several bugs with this test. It could highlight memory
 * leaks when repeatedly creating and destroying caches and/or managers, something that regular test would not find.
 */
public class MangleICacheTest {

    public enum Operation {
        CLOSE_CACHING_PROVIDER,

        CREATE_CACHE_MANAGER,
        CLOSE_CACHE_MANAGER,

        CREATE_CACHE,
        CLOSE_CACHE,
        DESTROY_CACHE,

        PUT
    }

    private static final ILogger LOGGER = Logger.getLogger(MangleICacheTest.class);

    public String basename = MangleICacheTest.class.getSimpleName();
    public int maxCaches = 100;

    public double createCacheManagerProb = 0.1;
    public double cacheManagerCloseProb = 0.1;
    public double cachingProviderCloseProb = 0.1;
    public double createCacheProb = 0.1;
    public double destroyCacheProb = 0.2;
    public double putCacheProb = 0.3;
    public double closeCacheProb = 0.1;

    private final OperationSelectorBuilder<Operation> operationSelectorBuilder = new OperationSelectorBuilder<Operation>();

    private HazelcastInstance hazelcastInstance;
    private IList<ICacheOperationCounter> results;

    @Setup
    public void setup(TestContext testContext) throws Exception {
        hazelcastInstance = testContext.getTargetInstance();
        results = hazelcastInstance.getList(basename);

        operationSelectorBuilder.addOperation(Operation.CREATE_CACHE_MANAGER, createCacheManagerProb)
                .addOperation(Operation.CLOSE_CACHE_MANAGER, cacheManagerCloseProb)
                .addOperation(Operation.CLOSE_CACHING_PROVIDER, cachingProviderCloseProb)
                .addOperation(Operation.CREATE_CACHE, createCacheProb)
                .addOperation(Operation.DESTROY_CACHE, destroyCacheProb)
                .addOperation(Operation.PUT, putCacheProb)
                .addOperation(Operation.CLOSE_CACHE, closeCacheProb);
    }

    @Verify(global = true)
    public void verify() throws Exception {
        ICacheOperationCounter total = new ICacheOperationCounter();
        for (ICacheOperationCounter counter : results) {
            total.add(counter);
        }
        LOGGER.info(basename + ": " + total + " from " + results.size() + " worker threads");
    }

    @RunWithWorker
    public Worker createWorker() {
        return new Worker();
    }

    private class Worker extends AbstractWorker<Operation> {

        private final CacheConfig config = new CacheConfig();
        private final ICacheOperationCounter counter = new ICacheOperationCounter();

        private CacheManager cacheManager;

        public Worker() {
            super(operationSelectorBuilder);

            config.setName(basename);
            createNewCacheManager();
        }

        @Override
        protected void timeStep(Operation operation) throws Exception {
            switch (operation) {
                case CLOSE_CACHING_PROVIDER:
                    closeCachingProvider();
                    break;
                case CREATE_CACHE_MANAGER:
                    createCacheManager();
                    break;
                case CLOSE_CACHE_MANAGER:
                    closeCacheManager();
                    break;
                case CREATE_CACHE:
                    createCache();
                    break;
                case CLOSE_CACHE:
                    closeCache();
                    break;
                case DESTROY_CACHE:
                    destroyCache();
                    break;
                case PUT:
                    put();
                    break;
                default:
                    throw new UnsupportedOperationException();
            }
        }

        private void closeCachingProvider() {
            try {
                CachingProvider provider = cacheManager.getCachingProvider();
                if (provider != null) {
                    provider.close();
                    counter.cachingProviderClose++;
                }
            } catch (CacheException e) {
                counter.cachingProviderCloseException++;
            }
        }

        private void createCacheManager() {
            try {
                createNewCacheManager();
                counter.createCacheManager++;
            } catch (CacheException e) {
                counter.createCacheManagerException++;
            }
        }

        private void closeCacheManager() {
            try {
                cacheManager.close();
                counter.cacheManagerClose++;
            } catch (CacheException e) {
                counter.cacheManagerCloseException++;
            }
        }

        private void createCache() {
            try {
                int cacheNumber = randomInt(maxCaches);
                cacheManager.createCache(basename + cacheNumber, config);
                counter.create++;
            } catch (CacheException e) {
                counter.createException++;
            } catch (IllegalStateException e) {
                counter.createException++;
            }
        }

        private void closeCache() {
            int cacheNumber = randomInt(maxCaches);
            Cache cache = getCacheIfExists(cacheNumber);
            try {
                if (cache != null) {
                    cache.close();
                    counter.cacheClose++;
                }
            } catch (CacheException e) {
                counter.cacheCloseException++;
            } catch (IllegalStateException e) {
                counter.cacheCloseException++;
            }
        }

        private void destroyCache() {
            try {
                int cacheNumber = randomInt(maxCaches);
                cacheManager.destroyCache(basename + cacheNumber);
                counter.destroy++;
            } catch (CacheException e) {
                counter.destroyException++;
            } catch (IllegalStateException e) {
                counter.destroyException++;
            }
        }

        private void put() {
            int cacheNumber = randomInt(maxCaches);
            Cache<Integer, Integer> cache = getCacheIfExists(cacheNumber);
            try {
                if (cache != null) {
                    cache.put(randomInt(), randomInt());
                    counter.put++;
                }
            } catch (CacheException e) {
                counter.getPutException++;
            } catch (IllegalStateException e) {
                counter.getPutException++;
            }
        }

        @Override
        protected void afterRun() {
            results.add(counter);
        }

        private void createNewCacheManager() {
            CachingProvider currentCachingProvider = null;
            if (cacheManager != null) {
                currentCachingProvider = cacheManager.getCachingProvider();
                cacheManager.close();
            }
            cacheManager = CacheUtils.createCacheManager(hazelcastInstance, currentCachingProvider);
        }

        private Cache<Integer, Integer> getCacheIfExists(int cacheNumber) {
            try {
                Cache<Integer, Integer> cache = cacheManager.getCache(basename + cacheNumber);
                counter.getCache++;
                return cache;

            } catch (CacheException e) {
                counter.getCacheException++;

            } catch (IllegalStateException e) {
                counter.getCacheException++;
            }
            return null;
        }
    }
}
