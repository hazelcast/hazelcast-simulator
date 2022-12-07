package com.hazelcast.simulator.tests.map.sql.realprod.client2;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.partition.PartitionAware;
import com.hazelcast.simulator.hz.IdentifiedDataSerializableFactory;

import java.io.IOException;

public class Client2IDSTransKey implements IdentifiedDataSerializable, PartitionAware<Long> {
    public Integer LogTranCtl1;
    public Integer LogTranCtl2;
    public Integer LogTranCtl3;
    public Integer LogTranCtl4;
    public Integer LogTranSortCode;
    public Long LogTranAcctNumber;

    @Override
    public Long getPartitionKey() {
        return LogTranAcctNumber;
    }

    @Override
    public int getFactoryId() {
        return IdentifiedDataSerializableFactory.FACTORY_ID;
    }

    @Override
    public int getClassId() {
        return IdentifiedDataSerializableFactory.CLIENT2_TRANS_KEY_TYPE;
    }

    @Override
    public void writeData(ObjectDataOutput objectDataOutput) throws IOException {
        objectDataOutput.writeInt(LogTranCtl1);
        objectDataOutput.writeInt(LogTranCtl2);
        objectDataOutput.writeInt(LogTranCtl3);
        objectDataOutput.writeInt(LogTranCtl4);
        objectDataOutput.writeInt(LogTranSortCode);
        objectDataOutput.writeLong(LogTranAcctNumber);
    }

    @Override
    public void readData(ObjectDataInput objectDataInput) throws IOException {
        LogTranCtl1 = objectDataInput.readInt();
        LogTranCtl2 = objectDataInput.readInt();
        LogTranCtl3 = objectDataInput.readInt();
        LogTranCtl4 = objectDataInput.readInt();
        LogTranSortCode = objectDataInput.readInt();
        LogTranAcctNumber = objectDataInput.readLong();
    }
}
