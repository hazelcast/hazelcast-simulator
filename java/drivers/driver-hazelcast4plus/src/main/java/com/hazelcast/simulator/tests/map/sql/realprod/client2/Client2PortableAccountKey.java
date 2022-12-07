package com.hazelcast.simulator.tests.map.sql.realprod.client2;

import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.partition.PartitionAware;

import java.io.IOException;

public class Client2PortableAccountKey implements Portable, PartitionAware<Long> {
    public Integer Account_Ctl1;
    public Integer Account_Ctl2;
    public Integer Account_Ctl3;
    public Integer Account_Ctl4;
    public Long Account_Number;

    @Override
    public Long getPartitionKey() {
        return Account_Number;
    }

    @Override
    public int getClassId() {
        return 3;
    }

    @Override
    public int getFactoryId() {
        return 1;
    }

    @Override
    public void readPortable(PortableReader reader) throws IOException {
        this.Account_Ctl1 = reader.readInt("Account_Ctl1");
        this.Account_Ctl2 = reader.readInt("Account_Ctl2");
        this.Account_Ctl3 = reader.readInt("Account_Ctl3");
        this.Account_Ctl4 = reader.readInt("Account_Ctl4");
        this.Account_Number = reader.readLong("Account_Number");
    }

    @Override
    public void writePortable(PortableWriter writer) throws IOException {
        writer.writeInt("Account_Ctl1", this.Account_Ctl1);
        writer.writeInt("Account_Ctl2", this.Account_Ctl2);
        writer.writeInt("Account_Ctl3", this.Account_Ctl3);
        writer.writeInt("Account_Ctl4", this.Account_Ctl4);
        writer.writeLong("Account_Number", this.Account_Number);
    }

}
