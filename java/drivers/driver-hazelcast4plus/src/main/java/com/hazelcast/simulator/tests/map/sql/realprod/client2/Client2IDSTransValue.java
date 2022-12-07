package com.hazelcast.simulator.tests.map.sql.realprod.client2;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.simulator.hz.IdentifiedDataSerializableFactory;

import java.io.IOException;
import java.math.BigDecimal;

public class Client2IDSTransValue implements IdentifiedDataSerializable {
    public BigDecimal LogTranAmount;
    public BigDecimal LogTranStmtBal;
    public Integer LogTranRecNum;
    public Integer LogTranBatch;
    public String LogRecordType;

    @Override
    public int getFactoryId() {
        return IdentifiedDataSerializableFactory.FACTORY_ID;
    }

    @Override
    public int getClassId() {
        return IdentifiedDataSerializableFactory.CLIENT2_TRANS_VALUE_TYPE;
    }

    @Override
    public void writeData(ObjectDataOutput objectDataOutput) throws IOException {
        objectDataOutput.writeObject(LogTranAmount);
        objectDataOutput.writeObject(LogTranStmtBal);
        objectDataOutput.writeObject(LogTranRecNum);
        objectDataOutput.writeObject(LogTranBatch);
        objectDataOutput.writeString(LogRecordType);
    }

    @Override
    public void readData(ObjectDataInput objectDataInput) throws IOException {
        LogTranAmount = objectDataInput.readObject();
        LogTranStmtBal = objectDataInput.readObject();
        LogTranRecNum = objectDataInput.readObject();
        LogTranBatch = objectDataInput.readObject();
        LogRecordType = objectDataInput.readString();
    }
}
