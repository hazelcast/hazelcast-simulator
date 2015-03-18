package com.hazelcast.simulator.provisioner.git;

import com.hazelcast.simulator.provisioner.Bash;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class BuildSupportTest {

    private Bash mockBash;
    private HazelcastJARFinder mockHazelcastJARFinder;

    private final File basePath = new File("/foo/bar");
    private final File[] expectedFiles = new File[]{new File("member.jar"), new File("client.jar")};

    @Before
    public void setUp() {
        mockBash = mock(Bash.class);
        mockHazelcastJARFinder = mock(HazelcastJARFinder.class);

        when(mockHazelcastJARFinder.find(basePath))
                .thenReturn(expectedFiles);
    }

    @Test
    public void build_defaultPathToMaven() {
        BuildSupport buildSupport = new BuildSupport(mockBash, mockHazelcastJARFinder);
        File[] returnedFiles = buildSupport.build(basePath);

        verify(mockBash).execute("cd /foo/bar ; mvn clean install -DskipTests");
        assertArrayEquals(expectedFiles, returnedFiles);
    }

}
