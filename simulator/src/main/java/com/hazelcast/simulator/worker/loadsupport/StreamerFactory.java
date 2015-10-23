/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast.simulator.worker.loadsupport;

import com.hazelcast.cache.ICache;
import com.hazelcast.core.IMap;
import com.hazelcast.instance.BuildInfo;
import com.hazelcast.instance.BuildInfoProvider;
import com.hazelcast.simulator.utils.EmptyStatement;

import javax.cache.Cache;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.simulator.utils.VersionUtils.isMinVersion;

/**
 * Creates {@link Streamer} instances for {@link IMap} and {@link Cache}.
 *
 * If possible an asynchronous variant is created, otherwise it will be synchronous.
 */
public final class StreamerFactory {

    private static final AtomicBoolean CREATE_ASYNC;

    static {
        boolean createAsync = false;
        try {
            BuildInfo buildInfo = BuildInfoProvider.getBuildInfo();
            if (isMinVersion("3.5", buildInfo.getVersion())) {
                createAsync = true;
            }
        } catch (NoClassDefFoundError e) {
            // it's Hazelcast 3.2 or older -> we have to use sync API
            EmptyStatement.ignore(e);
        } finally {
            CREATE_ASYNC = new AtomicBoolean(createAsync);
        }
    }

    private StreamerFactory() {
    }

    public static <K, V> Streamer<K, V> getInstance(IMap<K, V> map) {
        if (CREATE_ASYNC.get()) {
            return new AsyncMapStreamer<K, V>(map);
        }
        return new SyncMapStreamer<K, V>(map);
    }

    public static <K, V> Streamer<K, V> getInstance(Cache<K, V> cache) {
        if (CREATE_ASYNC.get() && cache instanceof ICache) {
            return new AsyncCacheStreamer<K, V>((ICache<K, V>) cache);
        }
        return new SyncCacheStreamer<K, V>(cache);
    }

    static void enforceAsync(boolean enforceAsync) {
        CREATE_ASYNC.set(enforceAsync);
    }
}
