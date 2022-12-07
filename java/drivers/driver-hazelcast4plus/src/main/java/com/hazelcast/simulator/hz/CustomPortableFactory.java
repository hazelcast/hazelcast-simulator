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

import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableFactory;
import com.hazelcast.simulator.tests.map.sql.realprod.client2.Client2PortableAccountKey;
import com.hazelcast.simulator.tests.map.sql.realprod.client2.Client2PortableAccountValue;
import com.hazelcast.simulator.tests.map.sql.realprod.client2.Client2PortableTransKey;
import com.hazelcast.simulator.tests.map.sql.realprod.client2.Client2PortableTransValue;

public class CustomPortableFactory implements PortableFactory {
    public static final int FACTORY_ID = 1;

    @Override
    public Portable create(int i) {
        switch (i) {
            case 1:
                return new LongPortablePojo();
            case 2:
                return new Client2PortableAccountValue();
            case 3:
                return new Client2PortableAccountKey();
            case 4:
                return new Client2PortableTransValue();
            case 5:
                return new Client2PortableTransKey();
        }
        return null;
    }
}
