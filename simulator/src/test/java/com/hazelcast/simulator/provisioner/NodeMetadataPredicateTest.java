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
package com.hazelcast.simulator.provisioner;

import com.hazelcast.simulator.protocol.registry.AgentData;
import com.hazelcast.simulator.protocol.registry.ComponentRegistry;
import org.jclouds.compute.domain.NodeMetadata;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.singleton;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class NodeMetadataPredicateTest {

    private Map<String, AgentData> terminateMap = new HashMap<String, AgentData>();
    private ComponentRegistry componentRegistry = new ComponentRegistry();

    private NodeMetadata nodeMetadata;

    @Before
    public void before() {
        componentRegistry.addAgent("172.16.16.1", "127.0.0.1");

        nodeMetadata = mock(NodeMetadata.class);
        when(nodeMetadata.getPublicAddresses()).thenReturn(singleton("172.16.16.1"));
    }

    @Test
    public void testApply_true() {
        terminateMap.put("172.16.16.1", componentRegistry.getFirstAgent());

        NodeMetadataPredicate nodeMetadataPredicate = new NodeMetadataPredicate(componentRegistry, terminateMap);

        assertTrue(nodeMetadataPredicate.apply(nodeMetadata));
    }

    @Test
    public void testApply_false() {
        terminateMap.put("172.16.16.2", componentRegistry.getFirstAgent());

        NodeMetadataPredicate nodeMetadataPredicate = new NodeMetadataPredicate(componentRegistry, terminateMap);

        assertFalse(nodeMetadataPredicate.apply(nodeMetadata));
    }
}
