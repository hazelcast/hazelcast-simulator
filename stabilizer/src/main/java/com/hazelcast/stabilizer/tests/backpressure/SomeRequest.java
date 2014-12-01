package com.hazelcast.stabilizer.tests.backpressure;

import com.hazelcast.client.impl.client.PartitionClientRequest;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.spi.Operation;

import java.io.IOException;
import java.security.Permission;

public class SomeRequest extends PartitionClientRequest {
    int partitionId;
    int syncBackupCount;
    int asyncBackupCount;
    long backupDelayNanos;

    public SomeRequest() {
    }

    public SomeRequest(int syncBackupCount, int asyncBackupCount, long backupDelayNanos) {
        this.syncBackupCount = syncBackupCount;
        this.asyncBackupCount = asyncBackupCount;
        this.backupDelayNanos = backupDelayNanos;
    }

    public void setPartitionId(int partitionId) {
        this.partitionId = partitionId;
    }

    @Override
    protected Operation prepareOperation() {
        SomeOperation op = new SomeOperation(syncBackupCount, asyncBackupCount, backupDelayNanos);
        op.setPartitionId(partitionId);
        return op;
    }

    @Override
    protected int getPartition() {
        return partitionId;
    }

    @Override
    public String getServiceName() {
        return null;
    }

    @Override
    public int getFactoryId() {
        return SomeRequestPortableFactory.FACTORY_ID;
    }

    @Override
    public int getClassId() {
        return 1;
    }

    @Override
    public Permission getRequiredPermission() {
        return null;
    }

    @Override
    public void write(PortableWriter writer) throws IOException {
        super.write(writer);

        writer.writeInt("syncBackupCount", syncBackupCount);
        writer.writeInt("asyncBackupCount", asyncBackupCount);
        writer.writeLong("backupDelayNanos", backupDelayNanos);
        writer.writeInt("partitionId", partitionId);
    }

    @Override
    public void read(PortableReader reader) throws IOException {
        super.read(reader);

        syncBackupCount = reader.readInt("syncBackupCount");
        asyncBackupCount = reader.readInt("asyncBackupCount");
        backupDelayNanos = reader.readLong("backupDelayNanos");
        partitionId = reader.readInt("partitionId");
    }
}
