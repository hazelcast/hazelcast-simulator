/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
