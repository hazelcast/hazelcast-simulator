package com.hazelcast.simulator.tests.syntheticmap;

import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.AbstractDistributedObject;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.OperationService;

public class SyntheticMapProxy<K,V> extends AbstractDistributedObject implements SyntheticMap<K,V> {

    private final String name;

    public SyntheticMapProxy(String name, NodeEngine nodeEngine, SyntheticMapService service){
        super(nodeEngine, service);
        this.name = name;
    }

    @Override
    public String getServiceName() {
        return SyntheticMapService.SERVICE_NAME;
    }

    @Override
    public V get(K key) {
        NodeEngine nodeEngine = getNodeEngine();
        Data keyData = nodeEngine.toData(key);
        int partitionId = nodeEngine.getPartitionService().getPartitionId(key);
        OperationService operationService = nodeEngine.getOperationService();
        GetOperation op = new GetOperation(name, keyData);
        InternalCompletableFuture f = operationService.invokeOnPartition(SyntheticMapService.SERVICE_NAME, op, partitionId);
        return (V)f.getSafely();
    }

    @Override
    public void put(K key, V value) {
        NodeEngine nodeEngine = getNodeEngine();
        Data keyData = nodeEngine.toData(key);
        Data valueData = nodeEngine.toData(value);

        int partitionId = nodeEngine.getPartitionService().getPartitionId(key);
        OperationService operationService = nodeEngine.getOperationService();
        PutOperation op = new PutOperation(name, keyData, valueData);
        InternalCompletableFuture f = operationService.invokeOnPartition(SyntheticMapService.SERVICE_NAME, op, partitionId);
        f.getSafely();
    }

    @Override
    public String getName() {
        return name;
    }
}
