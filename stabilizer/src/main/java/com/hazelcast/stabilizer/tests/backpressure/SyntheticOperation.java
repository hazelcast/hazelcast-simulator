package com.hazelcast.stabilizer.tests.backpressure;

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

    private int syncBackupCount;
    private int asyncBackupCount;
    private long backupOperationDelayNanos;

    public SyntheticOperation() {
    }

    public SyntheticOperation(int syncBackupCount, int asyncBackupCount, long backupOperationDelayNanos) {
        this.syncBackupCount = syncBackupCount;
        this.asyncBackupCount = asyncBackupCount;
        this.backupOperationDelayNanos = backupOperationDelayNanos;
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
        SyntheticBackupOperation syntheticBackupOperation = new SyntheticBackupOperation(backupOperationDelayNanos);
        syntheticBackupOperation.setPartitionId(getPartitionId());
        return syntheticBackupOperation;
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
        out.writeInt(syncBackupCount);
        out.writeInt(asyncBackupCount);
        out.writeLong(backupOperationDelayNanos);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);

        syncBackupCount = in.readInt();
        asyncBackupCount = in.readInt();
        backupOperationDelayNanos = in.readLong();
    }
}
