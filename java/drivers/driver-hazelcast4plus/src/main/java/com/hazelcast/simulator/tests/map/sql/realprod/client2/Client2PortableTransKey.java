package com.hazelcast.simulator.tests.map.sql.realprod.client2;

import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.partition.PartitionAware;

import java.io.IOException;

public class Client2PortableTransKey implements Portable, PartitionAware<Long> {
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
    public int getClassId() {
        return 5;
    }

    @Override
    public int getFactoryId() {
        return 1;
    }

    @Override
    public void readPortable(PortableReader reader) throws IOException {
        this.LogTranCtl1 = reader.readInt("LogTranCtl1");
        this.LogTranCtl2 = reader.readInt("LogTranCtl2");
        this.LogTranCtl3 = reader.readInt("LogTranCtl3");
        this.LogTranCtl4 = reader.readInt("LogTranCtl4");
        this.LogTranSortCode = reader.readInt("LogTranSortCode");
        this.LogTranAcctNumber = reader.readLong("LogTranAcctNumber");
    }

    @Override
    public void writePortable(PortableWriter writer) throws IOException {
        writer.writeInt("LogTranCtl1", this.LogTranCtl1);
        writer.writeInt("LogTranCtl2", this.LogTranCtl2);
        writer.writeInt("LogTranCtl3", this.LogTranCtl3);
        writer.writeInt("LogTranCtl4", this.LogTranCtl4);
        writer.writeInt("LogTranSortCode", this.LogTranSortCode);
        writer.writeLong("LogTranAcctNumber", this.LogTranAcctNumber);
    }

}
