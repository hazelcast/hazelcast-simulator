package com.hazelcast.simulator.utils.jars;

import com.hazelcast.simulator.utils.CommandLineExitException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

import static com.hazelcast.simulator.utils.FileUtils.deleteQuiet;
import static com.hazelcast.simulator.utils.FileUtils.ensureExistingDirectory;
import static com.hazelcast.simulator.utils.FileUtils.ensureExistingFile;
import static org.junit.Assert.assertEquals;

public class HazelcastJARFinderTest {

    private HazelcastJARFinder hazelcastJARFinder = new HazelcastJARFinder();

    private File path = new File("jarFinder");
    private File existingFile = new File(path, "hazelcast/target/hazelcast-3.6.jar");

    @Before
    public void setUp() {
        ensureExistingDirectory(path, "hazelcast/target");
        ensureExistingFile(existingFile);

        ensureExistingDirectory(path, "multipleJARs/target");
        ensureExistingFile(path, "multipleJARs/target/hazelcast-3.6.jar");
        ensureExistingFile(path, "multipleJARs/target/hazelcast-3.7.jar");

        ensureExistingDirectory(path, "noJAR/target");

        ensureExistingDirectory(path, "isFile");
        ensureExistingFile(path, "isFile/target");
    }

    @After
    public void tearDown() {
        deleteQuiet(path);
    }

    @Test
    public void testFind() {
        File[] hazelcastJARs = hazelcastJARFinder.find(path, new String[]{"hazelcast"});
        assertEquals(1, hazelcastJARs.length);
        assertEquals(existingFile.getPath(), hazelcastJARs[0].getPath());
    }

    @Test(expected = CommandLineExitException.class)
    public void testFind_multipleJARs() {
        hazelcastJARFinder.find(path, new String[]{"multipleJARs"});
    }

    @Test(expected = CommandLineExitException.class)
    public void testFind_jarNotExists() {
        hazelcastJARFinder.find(path, new String[]{"noJAR"});
    }

    @Test(expected = CommandLineExitException.class)
    public void testFind_targetIsFile() {
        hazelcastJARFinder.find(path, new String[]{"isFile"});
    }

    @Test(expected = CommandLineExitException.class)
    public void testFind_pathNotExists() {
        hazelcastJARFinder.find(new File("notExists"));
    }
}
