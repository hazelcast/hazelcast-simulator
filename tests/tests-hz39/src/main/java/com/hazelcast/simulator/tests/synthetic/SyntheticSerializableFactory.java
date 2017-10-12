/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hazelcast.simulator.tests.synthetic;

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
