package com.hazelcast.stabilizer.tests.backpressure;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.AbstractOperation;
import com.hazelcast.spi.BackupAwareOperation;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.PartitionAwareOperation;

import java.io.IOException;

public class SomeOperation extends AbstractOperation implements BackupAwareOperation, PartitionAwareOperation {
    private int syncBackupCount;
    private int asyncBackupCount;
    private long backupOperationDelayNanos;

    public SomeOperation() {
    }

    public SomeOperation(int syncBackupCount, int asyncBackupCount, long backupOperationDelayNanos) {
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
        SomeBackupOperation someBackupOperation = new SomeBackupOperation(backupOperationDelayNanos);
        someBackupOperation.setPartitionId(getPartitionId());
        return someBackupOperation;
    }

    @Override
    public void run() throws Exception {
        //do nothing
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
