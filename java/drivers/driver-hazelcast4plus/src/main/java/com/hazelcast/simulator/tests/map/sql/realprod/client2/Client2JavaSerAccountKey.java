package com.hazelcast.simulator.tests.map.sql.realprod.client2;

import com.hazelcast.partition.PartitionAware;

import java.io.Serializable;

public class Client2JavaSerAccountKey implements Serializable, PartitionAware<Long> {
    public Integer Account_Ctl1;
    public Integer Account_Ctl2;
    public Integer Account_Ctl3;
    public Integer Account_Ctl4;
    public Long Account_Number;

    @Override
    public Long getPartitionKey() {
        return Account_Number;
    }
}
