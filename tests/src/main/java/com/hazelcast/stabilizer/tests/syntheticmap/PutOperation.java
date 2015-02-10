package com.hazelcast.stabilizer.tests.syntheticmap;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.AbstractOperation;
import com.hazelcast.spi.PartitionAwareOperation;

import java.io.IOException;

public class PutOperation extends AbstractOperation implements PartitionAwareOperation, IdentifiedDataSerializable{
    private String mapName;
    private Data key;
    private Data value;

    public PutOperation(){}

    public PutOperation(String mapName, Data key, Data value){
        this.mapName = mapName;
        this.key = key;
        this.value = value;
    }

    @Override
    public void run() throws Exception {
        SyntheticMapService service = getService();
        service.put(getPartitionId(), mapName, key, value);
    }

    @Override
    public String getServiceName() {
        return SyntheticMapService.SERVICE_NAME;
    }

    @Override
    public int getFactoryId() {
        return SyntheticMapSerializableFactory.ID;
    }

    @Override
    public int getId() {
        return SyntheticMapSerializableFactory.PUT_OPERATION;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeUTF(mapName);
        out.writeData(key);
        out.writeData(value);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        mapName = in.readUTF();
        key = in.readData();
        value = in.readData();
    }
}
