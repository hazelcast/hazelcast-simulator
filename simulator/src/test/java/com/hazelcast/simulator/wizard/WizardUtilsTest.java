/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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
    public void before() {
        parent = new File("parent").getAbsoluteFile();
        ensureExistingDirectory(parent);
    }

    @After
    public void after() {
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
