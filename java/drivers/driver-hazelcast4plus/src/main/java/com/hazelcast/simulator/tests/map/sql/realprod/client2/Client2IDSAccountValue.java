package com.hazelcast.simulator.tests.map.sql.realprod.client2;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.simulator.hz.IdentifiedDataSerializableFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.LocalDateTime;

public class Client2IDSAccountValue implements IdentifiedDataSerializable {
    public String LOG_ACCT_APP_CD;
    public String LOG_ACCT_BRANCH_CD;
    public BigDecimal LOG_ACCT_OPEN_BAL;
    public BigDecimal LOG_ACCT_CURR_BAL;
    public LocalDateTime LOG_TIMESTAMP;
    public LocalDateTime CACHE_TIMESTAMP;

    @Override
    public int getFactoryId() {
        return IdentifiedDataSerializableFactory.FACTORY_ID;
    }

    @Override
    public int getClassId() {
        return IdentifiedDataSerializableFactory.CLIENT2_ACCOUNT_VALUE_TYPE;
    }

    @Override
    public void writeData(ObjectDataOutput objectDataOutput) throws IOException {
        objectDataOutput.writeString(LOG_ACCT_APP_CD);
        objectDataOutput.writeString(LOG_ACCT_BRANCH_CD);
        objectDataOutput.writeObject(LOG_ACCT_OPEN_BAL);
        objectDataOutput.writeObject(LOG_ACCT_CURR_BAL);
        objectDataOutput.writeObject(LOG_TIMESTAMP);
        objectDataOutput.writeObject(CACHE_TIMESTAMP);
    }

    @Override
    public void readData(ObjectDataInput objectDataInput) throws IOException {
        LOG_ACCT_APP_CD = objectDataInput.readString();
        LOG_ACCT_BRANCH_CD = objectDataInput.readString();
        LOG_ACCT_OPEN_BAL = objectDataInput.readObject();
        LOG_ACCT_CURR_BAL = objectDataInput.readObject();
        LOG_TIMESTAMP = objectDataInput.readObject();
        CACHE_TIMESTAMP = objectDataInput.readObject();
    }
}