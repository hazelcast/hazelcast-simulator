package com.hazelcast.simulator.agent;

import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static com.hazelcast.simulator.utils.FileUtils.writeText;
import static java.io.File.createTempFile;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

public class JavaInstallationsRepositoryTest {

    private JavaInstallationsRepository repository;

    @Before
    public void setUp() {
        repository = new JavaInstallationsRepository();
        File file = writeToTempFile(""
                + "1.vendor=sun\n"
                + "1.version=1.5\n"
                + "1.javaHome=/tmp\n"
                + "2.vendor=sun\n"
                + "2.version=1.6\n"
                + "2.javaHome=/tmp\n"
                + "3.vendor=openjdk\n"
                + "3.version=1.5\n"
                + "3.javaHome=/tmp\n"
        );
        repository.load(file);
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
        File file = writeToTempFile("1=sun\n");
        repository.load(file);
    }

    @Test(expected = JavaInstallationException.class)
    public void testLoad_unknownKey() {
        File file = writeToTempFile("1.foobar=sun\n");
        repository.load(file);
    }

    @Test(expected = JavaInstallationException.class)
    public void testLoad_missingVendor() {
        File file = writeToTempFile(""
                + "1.version=1.5\n"
                + "1.javaHome=/tmp\n"
        );
        repository.load(file);
    }

    @Test(expected = JavaInstallationException.class)
    public void testLoad_missingVersion() {
        File file = writeToTempFile(""
                + "1.vendor=sun\n"
                + "1.javaHome=/tmp\n"
        );
        repository.load(file);
    }

    @Test(expected = JavaInstallationException.class)
    public void testLoad_missingJavaHome() {
        File file = writeToTempFile(""
                + "1.vendor=sun\n"
                + "1.version=1.5\n"
        );
        repository.load(file);
    }

    private static File writeToTempFile(String text) {
        try {
            File file = createTempFile("test", "test");
            file.deleteOnExit();
            writeText(text, file);
            return file;
        } catch (IOException e) {
            fail("Could not create temp file: " + e.getMessage());
        }
        return null;
    }
}
