package com.hazelcast.simulator.tests.syntheticmap;

import com.hazelcast.core.DistributedObject;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.ManagedService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.RemoteService;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class SyntheticMapService implements ManagedService, RemoteService {

    public static final String SERVICE_NAME = "hz:impl:syntheticMapService";

    private NodeEngine nodeEngine;
    private Partition[] partitions;

    @Override
    public DistributedObject createDistributedObject(String objectName) {
        return new SyntheticMapProxy(objectName, nodeEngine, this);
    }

    @Override
    public void destroyDistributedObject(String objectName) {

    }

    @Override
    public void init(NodeEngine nodeEngine, Properties properties) {
        this.nodeEngine = nodeEngine;
        int partitionCount = nodeEngine.getPartitionService().getPartitionCount();
        partitions = new Partition[partitionCount];
        for (int k = 0; k < partitionCount; k++) {
            partitions[k] = new Partition();
        }
    }

    public Data get(int partitionId, String mapName, Data key) {
        Partition partition = partitions[partitionId];
        RecordStore recordStore = partition.get(mapName);
        return recordStore.get(key);
    }

    public void put(int partitionId, String mapName, Data key, Data value) {
        Partition partition = partitions[partitionId];
        RecordStore recordStore = partition.get(mapName);
        recordStore.put(key, value);
    }

    @Override
    public void reset() {
        //no-op
    }

    @Override
    public void shutdown(boolean terminate) {
        //no-op
    }

    private static class Partition {
        private Map<String,RecordStore> stores = new HashMap<String, RecordStore>();

        public RecordStore get(String name){
            RecordStore store = stores.get(name);
            if(store == null){
                store = new RecordStore();
                stores.put(name, store);
            }
            return  store;
        }
    }

    private static class RecordStore{
        private final Map<Data,Data> map = new HashMap<Data, Data>();

        public Data get(Data key) {
            return map.get(key);
        }

        public void put(Data key, Data value) {
            map.put(key, value);
        }
    }
}
