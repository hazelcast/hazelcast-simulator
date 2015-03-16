package com.hazelcast.simulator.tests.synthetic;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.AbstractOperation;
import com.hazelcast.spi.BackupAwareOperation;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.PartitionAwareOperation;

import java.io.IOException;

public class SyntheticOperation extends AbstractOperation
        implements BackupAwareOperation, PartitionAwareOperation, IdentifiedDataSerializable {

    private byte syncBackupCount;
    private byte asyncBackupCount;
    private long backupOperationDelayNanos;

    public SyntheticOperation() {
    }

    public SyntheticOperation(byte syncBackupCount, byte asyncBackupCount, long backupOperationDelayNanos) {
        this.syncBackupCount = syncBackupCount;
        this.asyncBackupCount = asyncBackupCount;
        this.backupOperationDelayNanos = backupOperationDelayNanos;
    }

    @Override
    public String getServiceName() {
        return null;
    }

    @Override
    public boolean shouldBackup() {
        return true;
    }

    @Override
    public int getSyncBackupCount() {
        return syncBackupCount;
    }

    @Override
    public int getAsyncBackupCount() {
        return asyncBackupCount;
    }

    @Override
    public Operation getBackupOperation() {
        SyntheticBackupOperation backupOperation = new SyntheticBackupOperation(backupOperationDelayNanos);
        backupOperation.setPartitionId(getPartitionId());
        return backupOperation;
    }

    @Override
    public void run() throws Exception {
        //do nothing
    }

    @Override
    public int getFactoryId() {
        return SyntheticSerializableFactory.ID;
    }

    @Override
    public int getId() {
        return SyntheticSerializableFactory.OPERATION;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeByte(syncBackupCount);
        out.writeByte(asyncBackupCount);
        out.writeLong(backupOperationDelayNanos);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        syncBackupCount = in.readByte();
        asyncBackupCount = in.readByte();
        backupOperationDelayNanos = in.readLong();
    }
}
