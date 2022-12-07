package com.hazelcast.simulator.tests.map.sql.realprod.client2;

import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.LocalDateTime;

public class Client2PortableAccountValue implements Portable {
    public String LOG_ACCT_APP_CD;
    public String LOG_ACCT_BRANCH_CD;
    public BigDecimal LOG_ACCT_OPEN_BAL;
    public BigDecimal LOG_ACCT_CURR_BAL;
    public LocalDateTime LOG_TIMESTAMP;
    public LocalDateTime CACHE_TIMESTAMP;

	@Override
	public int getClassId() {
		return 2;
	}

	@Override
	public int getFactoryId() {
		return 1;
	}

	@Override
	public void readPortable(PortableReader reader) throws IOException {
		this.LOG_ACCT_APP_CD = reader.readString("LOG_ACCT_APP_CD");
        this.LOG_ACCT_BRANCH_CD = reader.readString("LOG_ACCT_BRANCH_CD");
        this.LOG_ACCT_OPEN_BAL = reader.readDecimal("LOG_ACCT_OPEN_BAL");
        this.LOG_ACCT_CURR_BAL = reader.readDecimal("LOG_ACCT_CURR_BAL");
        this.LOG_TIMESTAMP = reader.readTimestamp("LOG_TIMESTAMP");
        this.CACHE_TIMESTAMP = reader.readTimestamp("CACHE_TIMESTAMP");
	}

	@Override
	public void writePortable(PortableWriter writer) throws IOException {
		writer.writeString("LOG_ACCT_APP_CD", this.LOG_ACCT_APP_CD);
		writer.writeString("LOG_ACCT_BRANCH_CD", this.LOG_ACCT_BRANCH_CD);
		writer.writeDecimal("LOG_ACCT_OPEN_BAL", this.LOG_ACCT_OPEN_BAL);
		writer.writeDecimal("LOG_ACCT_CURR_BAL", this.LOG_ACCT_CURR_BAL);
		writer.writeTimestamp("LOG_TIMESTAMP", this.LOG_TIMESTAMP);
		writer.writeTimestamp("CACHE_TIMESTAMP", this.CACHE_TIMESTAMP);
	}
}