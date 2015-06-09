package com.hazelcast.simulator.tests.syntheticmap;

import com.hazelcast.nio.serialization.Data;
import com.hazelcast.simulator.tests.helpers.HazelcastTestUtils;
import com.hazelcast.spi.AbstractDistributedObject;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.OperationService;

public class SyntheticMapProxy<K, V> extends AbstractDistributedObject<SyntheticMapService> implements SyntheticMap<K, V> {

    private final String name;
    private final NodeEngine nodeEngine;

    public SyntheticMapProxy(String name, NodeEngine nodeEngine, SyntheticMapService service) {
        super(nodeEngine, service);
        this.name = name;
        this.nodeEngine = nodeEngine;
    }

    @Override
    public String getServiceName() {
        return SyntheticMapService.SERVICE_NAME;
    }

    @Override
    public V get(K key) {
        Data keyData = nodeEngine.toData(key);

        GetOperation operation = new GetOperation(name, keyData);
        int partitionId = nodeEngine.getPartitionService().getPartitionId(key);

        OperationService operationService = HazelcastTestUtils.getOperationService(nodeEngine.getHazelcastInstance());
        return operationService
                .<V>invokeOnPartition(SyntheticMapService.SERVICE_NAME, operation, partitionId)
                .getSafely();
    }

    @Override
    public void put(K key, V value) {
        Data keyData = nodeEngine.toData(key);
        Data valueData = nodeEngine.toData(value);

        PutOperation operation = new PutOperation(name, keyData, valueData);
        int partitionId = nodeEngine.getPartitionService().getPartitionId(key);

        OperationService operationService = HazelcastTestUtils.getOperationService(nodeEngine.getHazelcastInstance());
        operationService
                .<V>invokeOnPartition(SyntheticMapService.SERVICE_NAME, operation, partitionId)
                .getSafely();
    }

    @Override
    public String getName() {
        return name;
    }
}
