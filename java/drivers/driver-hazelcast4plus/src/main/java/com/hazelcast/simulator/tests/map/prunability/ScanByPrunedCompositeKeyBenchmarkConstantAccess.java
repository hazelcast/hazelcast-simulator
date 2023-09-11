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
package com.hazelcast.simulator.tests.map.prunability;

import com.hazelcast.cluster.Member;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.jet.impl.util.Util;
import com.hazelcast.partition.Partition;
import com.hazelcast.partition.PartitionService;
import com.hazelcast.spi.impl.NodeEngineImpl;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

public abstract class ScanByPrunedCompositeKeyBenchmarkConstantAccess extends ScanByPrunedCompositeKeyBenchmarkBase {

    @Override
    protected int computeKey(HazelcastInstance target, boolean isKeyLocal) {
        PartitionService partitionService = target.getPartitionService();
        NodeEngineImpl nodeEngine = Util.getNodeEngine(target);
        UUID uuid = target.getLocalEndpoint().getUuid();

        // compute local partitions of coordinator
        Set<Integer> localPartitions = new HashSet<>();
        for (Partition partition : partitionService.getPartitions()) {
            Member owner = partition.getOwner();
            if (owner != null && owner.getUuid().equals(uuid)) {
                localPartitions.add(partition.getPartitionId());
            }
        }

        // compute a key that is local to the coordinator
        // note: there still may be a situation where the key
        //  is not local to the coordinator due to a migration.
        for (int i = 0; i < entryCount; i++) {
            long l = i;
            Object[] constants = new Object[]{i, l};
            Data keyData = nodeEngine.getSerializationService()
                    .toData(constructAttributeBasedKey(constants), v -> v);

            int partitionId = nodeEngine.getPartitionService().getPartitionId(keyData);
            if (localPartitions.contains(partitionId)) {
                return i;
            }
        }
        throw new AssertionError("No key found");
    }

    @Override
    protected int prepareKey() {
        return key;
    }

    private static Object constructAttributeBasedKey(Object[] keyAttributes) {
        return keyAttributes.length == 1 ? keyAttributes[0] : keyAttributes;
    }
}

