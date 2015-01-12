package com.hazelcast.stabilizer.tests.backpressure;

import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

public class SyntheticSerializableFactory implements DataSerializableFactory {

    public static final int ID = 2000;

    public static final int OPERATION = 1;
    public static final int BACKUP_OPERATION = 2;

    @Override
    public IdentifiedDataSerializable create(int typeId) {
        switch (typeId) {
            case OPERATION:
                return new SyntheticOperation();
            case BACKUP_OPERATION:
                return new SyntheticBackupOperation();
            default:
                return null;
        }
    }
}
