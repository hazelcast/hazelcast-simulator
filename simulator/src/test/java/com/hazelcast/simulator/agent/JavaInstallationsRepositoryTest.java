package com.hazelcast.simulator.agent;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

import static com.hazelcast.simulator.utils.FileUtils.deleteQuiet;
import static com.hazelcast.simulator.utils.FileUtils.ensureExistingFile;
import static com.hazelcast.simulator.utils.FileUtils.writeText;
import static com.hazelcast.simulator.utils.FormatUtils.NEW_LINE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class JavaInstallationsRepositoryTest {

    private File repositoryFile;
    private JavaInstallationsRepository repository;

    @Before
    public void setUp() {
        repositoryFile = ensureExistingFile("JavaInstallationsRepositoryTest.tmp");
        repository = new JavaInstallationsRepository();
        writeToRepositoryFile(""
                + "1.vendor=sun" + NEW_LINE
                + "1.version=1.5" + NEW_LINE
                + "1.javaHome=/tmp" + NEW_LINE
                + "2.vendor=sun" + NEW_LINE
                + "2.version=1.6" + NEW_LINE
                + "2.javaHome=/tmp" + NEW_LINE
                + "3.vendor=openjdk" + NEW_LINE
                + "3.version=1.5" + NEW_LINE
                + "3.javaHome=/tmp" + NEW_LINE
        );
        repository.load(repositoryFile);
    }

    @After
    public void tearDown() {
        deleteQuiet(repositoryFile);
    }

    @Test
    public void testGetAny() {
        assertNotNull(repository.getAny());
    }

    @Test
    public void testGetAny_emptyRepository() {
        JavaInstallationsRepository emptyRepository = new JavaInstallationsRepository();
        assertNull(emptyRepository.getAny());
    }

    @Test
    public void testLoad() {
        JavaInstallation installation = repository.get("sun", "1.5");
        assertNotNull(installation);
        assertEquals("sun", installation.getVendor());
        assertEquals("1.5", installation.getVersion());
        assertEquals("/tmp", installation.getJavaHome());
        assertNotNull(installation.toString());

        installation = repository.get("openjdk", "1.5");
        assertNotNull(installation);
        assertEquals("openjdk", installation.getVendor());
        assertEquals("1.5", installation.getVersion());
        assertEquals("/tmp", installation.getJavaHome());
        assertNotNull(installation.toString());

        installation = repository.get("openjdk", "1.0");
        assertNull(installation);
    }

    @Test(expected = JavaInstallationException.class)
    public void testLoad_invalidKeyFormat() {
        writeToRepositoryFile("1=sun" + NEW_LINE);
        repository.load(repositoryFile);
    }

    @Test(expected = JavaInstallationException.class)
    public void testLoad_unknownKey() {
        writeToRepositoryFile("1.foobar=sun" + NEW_LINE);
        repository.load(repositoryFile);
    }

    @Test(expected = JavaInstallationException.class)
    public void testLoad_missingVendor() {
        writeToRepositoryFile(""
                + "1.version=1.5" + NEW_LINE
                + "1.javaHome=/tmp" + NEW_LINE
        );
        repository.load(repositoryFile);
    }

    @Test(expected = JavaInstallationException.class)
    public void testLoad_missingVersion() {
        writeToRepositoryFile(""
                + "1.vendor=sun" + NEW_LINE
                + "1.javaHome=/tmp" + NEW_LINE
        );
        repository.load(repositoryFile);
    }

    @Test(expected = JavaInstallationException.class)
    public void testLoad_missingJavaHome() {
        writeToRepositoryFile(""
                + "1.vendor=sun" + NEW_LINE
                + "1.version=1.5" + NEW_LINE
        );
        repository.load(repositoryFile);
    }

    private void writeToRepositoryFile(String text) {
        writeText(text, repositoryFile);
    }
}
