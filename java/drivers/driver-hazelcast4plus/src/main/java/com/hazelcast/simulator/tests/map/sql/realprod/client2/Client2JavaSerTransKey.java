package com.hazelcast.simulator.tests.map.sql.realprod.client2;

import com.hazelcast.partition.PartitionAware;

import java.io.Serializable;

public class Client2JavaSerTransKey implements Serializable, PartitionAware<Long> {
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
}
