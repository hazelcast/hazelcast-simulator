package com.hazelcast.simulator.tests.map.helpers.tasks;

import com.hazelcast.config.DeviceConfig;
import com.hazelcast.config.LocalDeviceConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.spi.impl.NodeEngineImpl;

import java.io.File;
import java.io.Serializable;
import java.util.Arrays;
import java.util.concurrent.Callable;

import static java.util.Objects.requireNonNull;

/**
 * This task removes all data from all configured LocalDevices
 **/
public class ClearTsDirectoryTask implements HazelcastInstanceAware, Callable<Void>, Serializable {
    private HazelcastInstance hazelcastInstance;

    @Override
    public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
        this.hazelcastInstance = hazelcastInstance;
    }

    @Override
    public Void call() {
        HazelcastInstanceImpl hzImpl = (HazelcastInstanceImpl) hazelcastInstance;
        NodeEngineImpl nodeEngine = hzImpl.node.getNodeEngine();
        nodeEngine.getConfig().getDeviceConfigs()
                .values()
                .stream()
                .filter(DeviceConfig::isLocal)
                .forEach(deviceConfig -> {
                    File baseDir = ((LocalDeviceConfig) deviceConfig).getBaseDir();
                    Arrays.stream(requireNonNull(baseDir.listFiles()))
                            .forEach(IOUtil::delete);
                });
        return null;
    }
}
