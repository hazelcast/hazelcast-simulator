package com.hazelcast.simulator.tests.synthetic;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.AbstractOperation;
import com.hazelcast.spi.BackupOperation;
import com.hazelcast.spi.PartitionAwareOperation;

import java.io.IOException;
import java.util.concurrent.locks.LockSupport;

public class SyntheticBackupOperation extends AbstractOperation
        implements BackupOperation, PartitionAwareOperation, IdentifiedDataSerializable {

    private long delayNs;

    public SyntheticBackupOperation() {
    }

    public SyntheticBackupOperation(long delayNs) {
        this.delayNs = delayNs;
    }

    @Override
    public String getServiceName() {
        return null;
    }

    @Override
    public void run() throws Exception {
        LockSupport.parkNanos(delayNs);
    }

    @Override
    public int getFactoryId() {
        return SyntheticSerializableFactory.ID;
    }

    @Override
    public int getId() {
        return SyntheticSerializableFactory.BACKUP_OPERATION;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeLong(delayNs);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        delayNs = in.readLong();
    }
}
