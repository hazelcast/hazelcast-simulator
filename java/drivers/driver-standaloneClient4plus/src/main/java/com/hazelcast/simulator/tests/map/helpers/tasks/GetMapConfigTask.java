package com.hazelcast.simulator.tests.map.helpers.tasks;

import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import com.hazelcast.spi.impl.NodeEngineImpl;

import java.io.Serializable;
import java.util.concurrent.Callable;

/**
 * This task gets map configuration from the cluster.
 * It can be used for pre-checks when config is not accessible from the client API.
 **/
public class GetMapConfigTask implements HazelcastInstanceAware, Callable<MapConfig>, Serializable {
    private HazelcastInstance hazelcastInstance;
    private final String mapName;

    public GetMapConfigTask(String mapName) {
        this.mapName = mapName;
    }

    @Override
    public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
        this.hazelcastInstance = hazelcastInstance;
    }

    @Override
    public MapConfig call() {
        HazelcastInstanceImpl hzImpl = (HazelcastInstanceImpl) hazelcastInstance;
        NodeEngineImpl nodeEngine = hzImpl.node.getNodeEngine();
        return nodeEngine.getConfig().getMapConfig(mapName);
    }
}
