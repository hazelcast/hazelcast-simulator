package com.hazelcast.simulator.tests.icache.helpers;

import java.io.Serializable;

public final class ICacheOperationCounter implements Serializable {

    public long createCacheManager;
    public long createCacheManagerException;

    public long getCache;
    public long put;
    public long create;
    public long destroy;
    public long cacheClose;

    public long getCacheException;
    public long getPutException;
    public long createException;
    public long destroyException;
    public long cacheCloseException;

    public long cacheManagerClose;
    public long cachingProviderClose;

    public long cacheManagerCloseException;
    public long cachingProviderCloseException;

    public void add(ICacheOperationCounter counter) {
        createCacheManager += counter.createCacheManager;
        createCacheManagerException += counter.createCacheManagerException;

        getCache += counter.getCache;
        put += counter.put;
        create += counter.create;
        destroy += counter.destroy;
        cacheClose += counter.cacheClose;

        getCacheException += counter.getCacheException;
        getPutException += counter.getPutException;
        createException += counter.createException;
        destroyException += counter.destroyException;
        cacheCloseException += counter.cacheCloseException;

        cacheManagerClose += counter.cacheManagerClose;
        cachingProviderClose += counter.cachingProviderClose;
        cacheManagerCloseException += counter.cacheManagerCloseException;
        cachingProviderCloseException += counter.cachingProviderCloseException;
    }

    public String toString() {
        return "ICacheOperationCounter{"
                + "createCacheManager=" + createCacheManager
                + ", createCacheManagerException=" + createCacheManagerException
                + ", getCache=" + getCache
                + ", put=" + put
                + ", create=" + create
                + ", destroy=" + destroy
                + ", cacheClose=" + cacheClose
                + ", getCacheException=" + getCacheException
                + ", getPutException=" + getPutException
                + ", createException=" + createException
                + ", destroyException=" + destroyException
                + ", cacheCloseException=" + cacheCloseException
                + ", cacheManagerClose=" + cacheManagerClose
                + ", cachingProviderClose=" + cachingProviderClose
                + ", cacheManagerCloseException=" + cacheManagerCloseException
                + ", cachingProviderCloseException=" + cachingProviderCloseException
                + '}';
    }
}
