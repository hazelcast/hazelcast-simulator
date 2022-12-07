package com.hazelcast.simulator.tests.map.sql.realprod.client2;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.partition.PartitionAware;
import com.hazelcast.simulator.hz.IdentifiedDataSerializableFactory;

import java.io.IOException;

public class Client2IDSAccountKey implements IdentifiedDataSerializable, PartitionAware<Long> {
    public Integer Account_Ctl1;
    public Integer Account_Ctl2;
    public Integer Account_Ctl3;
    public Integer Account_Ctl4;
    public Long Account_Number;

    @Override
    public int getFactoryId() {
        return IdentifiedDataSerializableFactory.FACTORY_ID;
    }

    @Override
    public int getClassId() {
        return IdentifiedDataSerializableFactory.CLIENT2_ACCOUNT_KEY_TYPE;
    }

    @Override
    public void writeData(ObjectDataOutput objectDataOutput) throws IOException {
        objectDataOutput.writeInt(Account_Ctl1);
        objectDataOutput.writeInt(Account_Ctl2);
        objectDataOutput.writeInt(Account_Ctl3);
        objectDataOutput.writeInt(Account_Ctl4);
        objectDataOutput.writeLong(Account_Number);
    }

    @Override
    public void readData(ObjectDataInput objectDataInput) throws IOException {
        Account_Ctl1 = objectDataInput.readInt();
        Account_Ctl2 = objectDataInput.readInt();
        Account_Ctl3 = objectDataInput.readInt();
        Account_Ctl4 = objectDataInput.readInt();
        Account_Number = objectDataInput.readLong();
    }

    @Override
    public Long getPartitionKey() {
        return Account_Number;
    }
}
