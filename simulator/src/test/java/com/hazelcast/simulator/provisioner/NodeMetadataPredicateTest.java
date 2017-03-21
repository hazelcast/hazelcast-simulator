package com.hazelcast.simulator.provisioner;

import com.hazelcast.simulator.coordinator.registry.AgentData;
import com.hazelcast.simulator.coordinator.registry.ComponentRegistry;
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
    private ComponentRegistry registry = new ComponentRegistry();

    private NodeMetadata nodeMetadata;

    @Before
    public void before() {
        registry.addAgent("172.16.16.1", "127.0.0.1");

        nodeMetadata = mock(NodeMetadata.class);
        when(nodeMetadata.getPublicAddresses()).thenReturn(singleton("172.16.16.1"));
    }

    @Test
    public void testApply_true() {
        terminateMap.put("172.16.16.1", registry.getFirstAgent());

        NodeMetadataPredicate nodeMetadataPredicate = new NodeMetadataPredicate(registry, terminateMap);

        assertTrue(nodeMetadataPredicate.apply(nodeMetadata));
    }

    @Test
    public void testApply_false() {
        terminateMap.put("172.16.16.2", registry.getFirstAgent());

        NodeMetadataPredicate nodeMetadataPredicate = new NodeMetadataPredicate(registry, terminateMap);

        assertFalse(nodeMetadataPredicate.apply(nodeMetadata));
    }
}
