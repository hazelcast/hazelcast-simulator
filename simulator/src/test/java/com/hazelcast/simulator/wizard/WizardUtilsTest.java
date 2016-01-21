package com.hazelcast.simulator.wizard;

import com.hazelcast.simulator.utils.CommandLineExitException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.net.URL;

import static com.hazelcast.simulator.utils.FileUtils.deleteQuiet;
import static com.hazelcast.simulator.utils.FileUtils.ensureExistingDirectory;
import static com.hazelcast.simulator.utils.FileUtils.writeText;
import static com.hazelcast.simulator.utils.ReflectionUtils.invokePrivateConstructor;
import static com.hazelcast.simulator.wizard.WizardUtils.getJarDir;
import static com.hazelcast.simulator.wizard.WizardUtils.getProfileFile;
import static com.hazelcast.simulator.wizard.WizardUtils.getSimulatorPath;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class WizardUtilsTest {

    private File parent;

    @Before
    public void setUp() throws Exception {
        parent = new File("parent").getAbsoluteFile();
        ensureExistingDirectory(parent);
    }

    @After
    public void tearDown() throws Exception {
        deleteQuiet(parent);
    }

    @Test
    public void testConstructor() throws Exception {
        invokePrivateConstructor(WizardUtils.class);
    }

    @Test
    public void testGetSimulatorPath() {
        String simulatorPath = getSimulatorPath();

        assertNotNull(simulatorPath);
    }

    @Test
    public void testGetProfileFile_withBashrc() {
        File expectedFile = new File(parent, ".bashrc");
        writeText("# empty file", expectedFile);

        File actualFile = getProfileFile(parent.getPath());

        assertEquals(expectedFile.getPath(), actualFile.getPath());
    }

    @Test
    public void testGetProfileFile_withProfile() {
        File expectedFile = new File(parent, ".profile");
        writeText("# empty file", expectedFile);

        File actualFile = getProfileFile(parent.getPath());

        assertEquals(expectedFile.getPath(), actualFile.getPath());
    }

    @Test(expected = CommandLineExitException.class)
    public void testGetProfileFile_notFound() {
        getProfileFile(parent.getPath());
    }

    @Test
    public void testGetJarDir() {
        File jarDir = getJarDir(WizardUtils.class);

        assertNotNull(jarDir);
        assertTrue(jarDir.isDirectory());
    }

    @Test
    public void testGetFileFromUrl_fromJar() throws Exception {
        URL url = new URL("jar:file:/target/classes/simulator.jar!/WizardUtils.java");

        File file = WizardUtils.getFileFromUrl(url, "WizardUtils");

        assertNotNull(file);
        assertEquals("file:/target/classes/simulator.jar!/WizardUtils.java", file.getPath());
    }

    @Test
    public void testGetFileFromUrl_fromJarFile() throws Exception {
        URL url = new URL("file:/target/classes/simulator.jar");

        File file = WizardUtils.getFileFromUrl(url, "WizardUtils");

        assertNotNull(file);
        assertEquals("/target/classes", file.getPath());
    }

    @Test
    public void testGetFileFromUrl_fromClass() throws Exception {
        URL url = new URL("file:/target/classes/");

        File file = WizardUtils.getFileFromUrl(url, "WizardUtils");

        assertNotNull(file);
        assertEquals("/target/classes", file.getPath());
    }
}
