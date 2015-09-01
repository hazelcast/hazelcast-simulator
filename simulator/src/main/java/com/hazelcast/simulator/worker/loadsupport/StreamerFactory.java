package com.hazelcast.simulator.worker.loadsupport;

import com.hazelcast.cache.ICache;
import com.hazelcast.core.IMap;
import com.hazelcast.instance.BuildInfo;
import com.hazelcast.instance.BuildInfoProvider;
import com.hazelcast.simulator.utils.EmptyStatement;

import javax.cache.Cache;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.simulator.utils.VersionUtils.isMinVersion;

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
            //it's Hazelcast 3.2 or older -> we have to use sync API
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
