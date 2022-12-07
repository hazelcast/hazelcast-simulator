/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast.simulator.hz;

import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.simulator.tests.map.sql.realprod.client2.Client2IDSAccountKey;
import com.hazelcast.simulator.tests.map.sql.realprod.client2.Client2IDSAccountValue;
import com.hazelcast.simulator.tests.map.sql.realprod.client2.Client2IDSTransKey;
import com.hazelcast.simulator.tests.map.sql.realprod.client2.Client2IDSTransValue;

public class IdentifiedDataSerializableFactory implements DataSerializableFactory {

    public static final int FACTORY_ID = 1;

    public static final int SAMPLE_STRING_TYPE = 1;
    public static final int SAMPLE_LONG_TYPE = 2;
    public static final int SAMPLE_MUTLIPLE_INTS_TYPE = 3;

    public static final int CLIENT2_ACCOUNT_KEY_TYPE = 4;
    public static final int CLIENT2_ACCOUNT_VALUE_TYPE = 5;
    public static final int CLIENT2_TRANS_KEY_TYPE = 6;
    public static final int CLIENT2_TRANS_VALUE_TYPE = 7;

    @Override
    public IdentifiedDataSerializable create(int typeId) {
        switch (typeId) {
            case SAMPLE_STRING_TYPE:
                return new IdentifiedDataSerializablePojo();
            case SAMPLE_LONG_TYPE:
                return new IdentifiedDataWithLongSerializablePojo();
            case SAMPLE_MUTLIPLE_INTS_TYPE:
                return new IdentifiedDataSerializableMultipleIntsPojo();
            case CLIENT2_ACCOUNT_KEY_TYPE:
                return new Client2IDSAccountKey();
            case CLIENT2_ACCOUNT_VALUE_TYPE:
                return new Client2IDSAccountValue();
            case CLIENT2_TRANS_KEY_TYPE:
                return new Client2IDSTransKey();
            case CLIENT2_TRANS_VALUE_TYPE:
                return new Client2IDSTransValue();
            default:
                return null;
        }
    }
}
