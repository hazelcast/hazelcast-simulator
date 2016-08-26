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
import static com.hazelcast.simulator.utils.FormatUtils.NEW_LINE;
import static com.hazelcast.simulator.utils.ReflectionUtils.invokePrivateConstructor;
import static com.hazelcast.simulator.wizard.WizardUtils.containsCommentedOutProperty;
import static com.hazelcast.simulator.wizard.WizardUtils.getCommentedOutProperty;
import static com.hazelcast.simulator.wizard.WizardUtils.getJarDir;
import static com.hazelcast.simulator.wizard.WizardUtils.getProfileFile;
import static com.hazelcast.simulator.wizard.WizardUtils.getSimulatorPath;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class WizardUtilsTest {

    private static final String BEFORE = "#" + NEW_LINE + "# Test property" + NEW_LINE + "#" + NEW_LINE;
    private static final String AFTER = NEW_LINE + NEW_LINE + "#" + NEW_LINE + "# next property..." + NEW_LINE + "#" + NEW_LINE;

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

    @Test
    public void testGetProfileFile_withZshrc() {
        File expectedFile = new File(parent, ".zshrc");
        writeText("# empty file", expectedFile);

        File actualFile = getProfileFile(parent.getPath());

        assertEquals(expectedFile.getPath(), actualFile.getPath());
    }

    @Test(expected = CommandLineExitException.class)
    public void testGetProfileFile_notFound() {
        getProfileFile(parent.getPath());
    }

    @Test
    public void testContainsCommentedOutProperty() {
        assertTrue(containsCommentedOutProperty(BEFORE + "#TEST_PROPERTY=foobar" + AFTER, "TEST_PROPERTY"));
        assertTrue(containsCommentedOutProperty(BEFORE + "# TEST_PROPERTY = foobar" + AFTER, "TEST_PROPERTY"));
        assertTrue(containsCommentedOutProperty(BEFORE + "  #  TEST_PROPERTY  = foobar " + AFTER, "TEST_PROPERTY"));

        assertFalse(containsCommentedOutProperty(BEFORE + "TEST_PROPERTY=foobar" + AFTER, "TEST_PROPERTY"));
        assertFalse(containsCommentedOutProperty(BEFORE + "TEST_PROPERTY = foobar" + AFTER, "TEST_PROPERTY"));
        assertFalse(containsCommentedOutProperty(BEFORE + "  TEST_PROPERTY  =  foobar " + AFTER, "TEST_PROPERTY"));

        assertFalse(containsCommentedOutProperty(BEFORE + "#TEST PROPERTY=foobar" + AFTER, "TEST_PROPERTY"));
        assertFalse(containsCommentedOutProperty(BEFORE + "# TEST PROPERTY = foobar" + AFTER, "TEST_PROPERTY"));
        assertFalse(containsCommentedOutProperty(BEFORE + "  t#  TEST PROPERTY  =  foobar " + AFTER, "TEST_PROPERTY"));
    }

    @Test
    public void testGetCommentedOutProperty() {
        assertEquals("foobar", getCommentedOutProperty(BEFORE + "#TEST_PROPERTY=foobar" + AFTER, "TEST_PROPERTY"));
        assertEquals("foobar", getCommentedOutProperty(BEFORE + "# TEST_PROPERTY = foobar" + AFTER, "TEST_PROPERTY"));
        assertEquals("foobar", getCommentedOutProperty(BEFORE + "  #  TEST_PROPERTY  = foobar " + AFTER, "TEST_PROPERTY"));

        assertNull(getCommentedOutProperty(BEFORE + "TEST_PROPERTY=foobar" + AFTER, "TEST_PROPERTY"));
        assertNull(getCommentedOutProperty(BEFORE + " TEST_PROPERTY = foobar" + AFTER, "TEST_PROPERTY"));
        assertNull(getCommentedOutProperty(BEFORE + "  TEST_PROPERTY  = foobar " + AFTER, "TEST_PROPERTY"));

        assertNull(getCommentedOutProperty(BEFORE + "#TEST PROPERTY=foobar" + AFTER, "TEST_PROPERTY"));
        assertNull(getCommentedOutProperty(BEFORE + "# TEST PROPERTY = foobar" + AFTER, "TEST_PROPERTY"));
        assertNull(getCommentedOutProperty(BEFORE + "  #  TEST PROPERTY  = foobar " + AFTER, "TEST_PROPERTY"));
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
