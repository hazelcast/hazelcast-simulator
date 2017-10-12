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
package com.hazelcast.simulator.tests.syntheticmap;

import com.hazelcast.nio.serialization.Data;
import com.hazelcast.simulator.tests.helpers.HazelcastTestUtils;
import com.hazelcast.spi.AbstractDistributedObject;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.OperationService;

public class SyntheticMapProxy<K, V> extends AbstractDistributedObject<SyntheticMapService> implements SyntheticMap<K, V> {

    private final String name;
    private final NodeEngine nodeEngine;

    public SyntheticMapProxy(String name, NodeEngine nodeEngine, SyntheticMapService service) {
        super(nodeEngine, service);
        this.name = name;
        this.nodeEngine = nodeEngine;
    }

    @Override
    public String getServiceName() {
        return SyntheticMapService.SERVICE_NAME;
    }

    @Override
    public V get(K key) {
        Data keyData = nodeEngine.toData(key);

        GetOperation operation = new GetOperation(name, keyData);
        int partitionId = nodeEngine.getPartitionService().getPartitionId(key);

        OperationService operationService = HazelcastTestUtils.getOperationService(nodeEngine.getHazelcastInstance());
        return operationService
                .<V>invokeOnPartition(SyntheticMapService.SERVICE_NAME, operation, partitionId)
                .join();
    }

    @Override
    public void put(K key, V value) {
        Data keyData = nodeEngine.toData(key);
        Data valueData = nodeEngine.toData(value);

        PutOperation operation = new PutOperation(name, keyData, valueData);
        int partitionId = nodeEngine.getPartitionService().getPartitionId(key);

        OperationService operationService = HazelcastTestUtils.getOperationService(nodeEngine.getHazelcastInstance());
        operationService
                .<V>invokeOnPartition(SyntheticMapService.SERVICE_NAME, operation, partitionId)
                .join();
    }

    @Override
    public String getName() {
        return name;
    }
}
