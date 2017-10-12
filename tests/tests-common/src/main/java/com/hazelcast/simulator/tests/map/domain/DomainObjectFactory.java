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
package com.hazelcast.simulator.tests.map.domain;

import com.hazelcast.simulator.tests.map.SerializationStrategyTest.Strategy;

public final class DomainObjectFactory {

    private final Strategy strategy;

    private DomainObjectFactory(Strategy strategy) {
        this.strategy = strategy;
    }

    public static DomainObjectFactory newFactory(Strategy strategy) {
        return new DomainObjectFactory(strategy);
    }

    public DomainObject newInstance() {
        switch (strategy) {
            case PORTABLE:
                return new PortableDomainObject();
            case SERIALIZABLE:
                return new SerializableDomainObject();
            case DATA_SERIALIZABLE:
                return new DataSerializableDomainObject();
            case IDENTIFIED_DATA_SERIALIZABLE:
                return new IdentifiedDataSerializableDomainObject();
            default:
                throw new IllegalStateException("Unknown strategy: " + strategy);
        }
    }
}
