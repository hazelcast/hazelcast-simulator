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
    public void setUp() throws Exception {
        ensureExistingDirectory(new File(path, "hazelcast/target"));
        ensureExistingFile(existingFile);

        ensureExistingDirectory(new File(path, "multipleJARs/target"));
        ensureExistingFile(new File(path, "multipleJARs/target/hazelcast-3.6.jar"));
        ensureExistingFile(new File(path, "multipleJARs/target/hazelcast-3.7.jar"));

        ensureExistingDirectory(new File(path, "noJAR/target"));

        ensureExistingDirectory(new File(path, "isFile"));
        ensureExistingFile(new File(path, "isFile/target"));
    }

    @After
    public void tearDown() throws Exception {
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
