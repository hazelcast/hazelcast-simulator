package com.hazelcast.simulator.utils.jars;

import com.hazelcast.simulator.provisioner.Bash;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

import static org.junit.Assert.assertArrayEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class BuildSupportTest {

    private final File basePath = new File("/foo/bar");
    private final File[] expectedFiles = new File[]{new File("member.jar"), new File("client.jar"), new File("wm.jar")};

    private Bash mockBash;
    private HazelcastJARFinder mockHazelcastJARFinder;

    @Before
    public void setUp() {
        mockBash = mock(Bash.class);
        mockHazelcastJARFinder = mock(HazelcastJARFinder.class);

        when(mockHazelcastJARFinder.find(basePath)).thenReturn(expectedFiles);
    }

    @Test
    public void build_defaultPathToMaven() {
        BuildSupport buildSupport = new BuildSupport(mockBash, mockHazelcastJARFinder);
        File[] returnedFiles = buildSupport.build(basePath);

        verify(mockBash).execute("cd /foo/bar; mvn clean install -DskipTests");
        assertArrayEquals(expectedFiles, returnedFiles);
    }
}
