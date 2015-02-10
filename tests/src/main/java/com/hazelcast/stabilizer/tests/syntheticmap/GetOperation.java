package com.hazelcast.stabilizer.tests.syntheticmap;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.AbstractOperation;
import com.hazelcast.spi.PartitionAwareOperation;

import java.io.IOException;

public class GetOperation extends AbstractOperation implements PartitionAwareOperation, IdentifiedDataSerializable {

    private String mapName;
    private Data key;
    private Data response;

    public GetOperation(){}

    public GetOperation(String mapName, Data key) {
        this.mapName = mapName;
        this.key = key;
    }

    @Override
    public String getServiceName() {
        return SyntheticMapService.SERVICE_NAME;
    }

    @Override
    public void run() throws Exception {
        SyntheticMapService mapService = getService();
        response = mapService.get(getPartitionId(), mapName, key);
    }

    @Override
    public Data getResponse() {
        return response;
    }

    @Override
    public int getFactoryId() {
        return SyntheticMapSerializableFactory.ID;
    }

    @Override
    public int getId() {
        return SyntheticMapSerializableFactory.GET_OPERATION;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeUTF(mapName);
        out.writeData(key);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        mapName = in.readUTF();
        key = in.readData();
    }
}
