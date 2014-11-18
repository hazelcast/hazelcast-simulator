package com.hazelcast.stabilizer.tests.backpressure;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.AbstractOperation;
import com.hazelcast.spi.BackupOperation;
import com.hazelcast.spi.PartitionAwareOperation;

import java.io.IOException;
import java.util.concurrent.locks.LockSupport;

public class SomeBackupOperation extends AbstractOperation implements BackupOperation, PartitionAwareOperation {
    private long delayNs;

    public SomeBackupOperation() {
    }

    public SomeBackupOperation(long delayNs) {
        this.delayNs = delayNs;
    }

    @Override
    public void run() throws Exception {
        LockSupport.parkNanos(delayNs);
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
