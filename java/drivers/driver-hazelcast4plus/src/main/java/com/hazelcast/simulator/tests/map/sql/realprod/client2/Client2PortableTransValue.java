package com.hazelcast.simulator.tests.map.sql.realprod.client2;

import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;

import java.io.IOException;
import java.math.BigDecimal;

public class Client2PortableTransValue implements Portable {
    public BigDecimal LogTranAmount;
    public BigDecimal LogTranStmtBal;
    public Integer LogTranRecNum;
    public Integer LogTranBatch;
    public String LogRecordType;

    @Override
    public int getClassId() {
        return 4;
    }

    @Override
    public int getFactoryId() {
        return 1;
    }

    @Override
    public void readPortable(PortableReader reader) throws IOException {
        this.LogTranAmount = reader.readDecimal("LogTranAmount");
        this.LogTranStmtBal = reader.readDecimal("LogTranStmtBal");
        this.LogTranRecNum = reader.readInt("LogTranRecNum");
        this.LogTranBatch = reader.readInt("LogTranBatch");
        this.LogRecordType = reader.readString("LogRecordType");
    }

    @Override
    public void writePortable(PortableWriter writer) throws IOException {
        writer.writeDecimal("LogTranAmount", this.LogTranAmount);
        writer.writeDecimal("LogTranStmtBal", this.LogTranStmtBal);
        writer.writeInt("LogTranRecNum", this.LogTranRecNum);
        writer.writeInt("LogTranBatch", this.LogTranBatch);
        writer.writeString("LogRecordType", this.LogRecordType);
    }
}
