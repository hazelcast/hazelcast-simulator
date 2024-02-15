package com.hazelcast.simulator.tests.map.helpers.tasks;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import com.hazelcast.spi.impl.NodeEngineImpl;

import java.io.Serializable;
import java.lang.reflect.Method;
import java.util.concurrent.Callable;

/**
 * This task gets value of HlogLengthExposer from the member.
 * <p>Reflection is used to build project without dependency on Enterprise Edition code.
 * <p>To get cluster-wise value it must be sent to all members and aggregated.
 **/
public class GetHybridLogLengthTask implements HazelcastInstanceAware, Callable<Long>, Serializable {
    private HazelcastInstance hazelcastInstance;
    private static final String SERVICE_NAME = "hz:ee:tieredStoreServiceImpl";

    @Override
    public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
        this.hazelcastInstance = hazelcastInstance;
    }

    @Override
    public Long call() throws ReflectiveOperationException {
        HazelcastInstanceImpl hzImpl = (HazelcastInstanceImpl) hazelcastInstance;
        NodeEngineImpl nodeEngine = hzImpl.node.getNodeEngine();

        Object serviceInstance = nodeEngine.getService(SERVICE_NAME);

        Method getMetrics = serviceInstance.getClass().getMethod("getMetrics");
        Object metrics = getMetrics.invoke(serviceInstance);

        Method getPerMemberMetrics = metrics.getClass().getMethod("getPerMemberMetrics");
        Object perMemberMetrics = getPerMemberMetrics.invoke(metrics);

        Method getCMethod = perMemberMetrics.getClass().getMethod("getHlogLengthExposer");
        return (Long) getCMethod.invoke(perMemberMetrics);
    }

}
