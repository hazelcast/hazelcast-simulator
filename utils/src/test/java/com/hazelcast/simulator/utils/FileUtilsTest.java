package com.hazelcast.simulator.utils;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

import static com.hazelcast.simulator.utils.FileUtils.USER_HOME;
import static com.hazelcast.simulator.utils.FileUtils.appendText;
import static com.hazelcast.simulator.utils.FileUtils.deleteQuiet;
import static com.hazelcast.simulator.utils.FileUtils.ensureExistingDirectory;
import static com.hazelcast.simulator.utils.FileUtils.ensureExistingFile;
import static com.hazelcast.simulator.utils.FileUtils.fileAsText;
import static com.hazelcast.simulator.utils.FileUtils.getResourceFile;
import static com.hazelcast.simulator.utils.FileUtils.isValidFileName;
import static com.hazelcast.simulator.utils.FileUtils.newFile;
import static com.hazelcast.simulator.utils.FileUtils.rename;
import static com.hazelcast.simulator.utils.FileUtils.writeText;
import static com.hazelcast.simulator.utils.ReflectionUtils.invokePrivateConstructor;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class FileUtilsTest {

    private static final File FILE_NOT_FOUND = new File("notFound");
    private static final File INVALID_FILE = new File("\\//?:&|");
    private static final File INACCESSIBLE_FILE = new File("/dev/null");

    private static final File EXAMPLE_FILE = new File("fileUtilsTestFile");
    private static final String EXAMPLE_CONTENT = "exampleContent";

    @Before
    public void setUp() {
        writeText(EXAMPLE_CONTENT, EXAMPLE_FILE);
    }

    @After
    public void tearDown() {
        deleteQuiet(EXAMPLE_FILE);
        deleteQuiet(INVALID_FILE);
    }

    @Test
    public void testConstructor() throws Exception {
        invokePrivateConstructor(FileUtils.class);
    }

    @Test
    public void testIsValidFileName() {
        assertTrue(isValidFileName("validFilename"));
        assertTrue(isValidFileName("validFilename23"));
        assertTrue(isValidFileName("42validFilename"));
        assertTrue(isValidFileName("VALID_FILENAME"));

        assertFalse(isValidFileName("INVALID FILENAME"));
        assertFalse(isValidFileName("invalid$filename"));
    }

    @Test
    public void testNewFile_withEmptyPath() {
        File actual = newFile("");
        assertEquals("", actual.getPath());
    }

    @Test
    public void testNewFile_withUserHome() {
        File actual = newFile("~");
        assertEquals(USER_HOME, actual.getPath());
    }

    @Test
    public void testNewFile_withUserHomeInString() {
        File actual = newFile("~" + File.separator + "foobar");
        assertEquals(USER_HOME + File.separator + "foobar", actual.getPath());
    }

    @Test
    public void testNewFile_withParent() {
        File parent = newFile("~");
        File actual = newFile(parent, "foobar");
        assertEquals(USER_HOME + File.separator + "foobar", actual.getPath());
    }

    @Test
    public void testNewFile_manyItems() {
        File actual = newFile("~", "foobar", "test");
        assertEquals(USER_HOME + File.separator + "foobar" + File.separator + "test", actual.getPath());
    }

    @Test
    public void testWriteText() {
        File file = new File("testWriteText");
        try {
            String expected = "write test";
            writeText(expected, file);

            String actual = fileAsText(file);
            assertEquals(expected, actual);
        } finally {
            deleteQuiet(file);
        }
    }

    @Test(expected = NullPointerException.class)
    public void testWriteText_withNullText() {
        writeText(null, new File("ignored"));
    }

    @Test(expected = NullPointerException.class)
    public void testWriteText_withNullFile() {
        writeText("ignored", null);
    }

    @Test(expected = FileUtilsException.class)
    public void testWriteText_withInvalidFilename() {
        writeText("ignored", INVALID_FILE);
    }

    @Test
    public void testAppendText() {
        File file = null;
        try {
            String expected = "write test";
            file = appendText(expected, "testAppendText");
            appendText(expected, file);

            String actual = fileAsText(file);
            assertEquals(expected + expected, actual);
        } finally {
            deleteQuiet(file);
        }
    }

    @Test(expected = NullPointerException.class)
    public void testAppendText_withNullText() {
        appendText(null, new File("ignored"));
    }

    @Test(expected = NullPointerException.class)
    public void testAppendText_withNullFile() {
        appendText("ignored", (File) null);
    }

    @Test(expected = FileUtilsException.class)
    public void testAppendText_withInvalidFilename() {
        appendText("ignored", INVALID_FILE);
    }

    @Test
    public void testGetText() throws Exception {
        String fileContent = FileUtils.getText(EXAMPLE_FILE.toURI().toURL().toExternalForm());

        assertEquals(EXAMPLE_CONTENT, fileContent);
    }

    @Test(expected = FileUtilsException.class)
    public void testGetText_fileNotFound() throws Exception {
        FileUtils.getText(FILE_NOT_FOUND.toURI().toURL().toExternalForm());
    }

    @Test
    public void testGetResourceFile() {
        String resourceFileContent = getResourceFile("fileUtilsTestFile.txt");
        assertEquals("testContent", resourceFileContent);
    }

    @Test(expected = FileUtilsException.class)
    public void testGetResourceFile_fileNotFound() {
        getResourceFile("notFound");
    }

    @Test
    public void testFileAsText_withFileName() {
        String actual = fileAsText(EXAMPLE_FILE.getAbsolutePath());
        assertEquals(EXAMPLE_CONTENT, actual);
    }

    @Test(expected = FileUtilsException.class)
    public void testFileAsText_withInvalidFileName() {
        fileAsText(INVALID_FILE.getAbsolutePath());
    }

    @Test
    public void testFileAsText_withFile() {
        String actual = fileAsText(EXAMPLE_FILE);
        assertEquals(EXAMPLE_CONTENT, actual);
    }

    @Test(expected = FileUtilsException.class)
    public void testFileAsText_withInvalidFile() {
        fileAsText(INVALID_FILE);
    }

    @Test
    public void testDeleteQuiet_withFile() {
        File file = ensureExistingFile("deleteQuietFile");
        assertTrue(file.exists());

        deleteQuiet(file.getAbsolutePath());

        assertFalse(file.exists());
    }

    @Test
    public void testDeleteQuiet_withDirectory() {
        File parent = ensureExistingDirectory("deleteQuietDirectory");
        File file = ensureExistingDirectory(parent, "deleteQuietFile");
        assertTrue(parent.exists());
        assertTrue(file.exists());

        deleteQuiet(parent.getAbsolutePath());

        assertFalse(parent.exists());
        assertFalse(file.exists());
    }

    @Test
    public void testDeleteQuiet_withInaccessibleFile() {
        deleteQuiet(INACCESSIBLE_FILE);
    }

    @Test
    public void testDeleteQuiet_withFileNotFound() {
        deleteQuiet(FILE_NOT_FOUND);
    }

    @Test
    public void testDeleteQuiet_withInvalidFile() {
        deleteQuiet(INVALID_FILE);
    }

    @Test
    public void testDeleteQuiet_withNullFile() {
        deleteQuiet((File) null);
    }

    @Test
    public void testEnsureExistingFile_withFileName() {
        File file = null;
        try {
            file = ensureExistingFile("ensureExistingFileTest");

            assertTrue(file.exists());
        } finally {
            deleteQuiet(file);
        }
    }

    @Test
    public void testEnsureExistingFile_withParent() {
        File parent = ensureExistingDirectory("parent");
        try {
            File file = ensureExistingFile(parent, "test");

            assertTrue(file.exists());
        } finally {
            deleteQuiet(parent);
        }
    }

    @Test
    public void testEnsureExistingFile_withExistingFile() {
        File file = null;
        try {
            file = ensureExistingFile("ensureExistingFileTest");
            assertTrue(file.exists());

            file = ensureExistingFile("ensureExistingFileTest");
            assertTrue(file.exists());
        } finally {
            deleteQuiet(file);
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testEnsureExistingFile_withDirectory() {
        File parent = ensureExistingDirectory("parent");
        try {
            ensureExistingFile(parent);
        } finally {
            deleteQuiet(parent);
        }
    }

    @Test(expected = FileUtilsException.class)
    public void testEnsureExistingFile_withInaccessibleFile() {
        ensureExistingFile(new File(INACCESSIBLE_FILE, "test"));
    }

    @Test(expected = FileUtilsException.class)
    public void testEnsureExistingFile_withInvalidFile() {
        ensureExistingFile(INVALID_FILE);
    }

    @Test(expected = FileUtilsException.class)
    public void testEnsureExistingFile_whenFileCouldNotBeCreated() throws Exception {
        File file = mock(File.class);
        when(file.isFile()).thenReturn(false);
        when(file.isDirectory()).thenReturn(false);
        when(file.exists()).thenReturn(false);
        when(file.createNewFile()).thenReturn(false);

        ensureExistingFile(file);
    }

    @Test
    public void testEnsureExistingDirectory_withFileName() {
        File file = null;
        try {
            file = ensureExistingDirectory("ensureExistingDirectoryTest");

            assertTrue(file.exists());
        } finally {
            deleteQuiet(file);
        }
    }

    @Test
    public void testEnsureExistingDirectory_withParent() {
        File parent = ensureExistingDirectory("parent");
        try {
            File file = ensureExistingDirectory(parent, "child");

            assertTrue(file.exists());
        } finally {
            deleteQuiet(parent);
        }
    }

    @Test
    public void testEnsureExistingDirectory_withExistingFile() {
        File file = null;
        try {
            file = ensureExistingDirectory("ensureExistingDirectoryTest");
            assertTrue(file.exists());

            file = ensureExistingDirectory("ensureExistingDirectoryTest");
            assertTrue(file.exists());
        } finally {
            deleteQuiet(file);
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testEnsureExistingDirectory_withFile() {
        File parent = ensureExistingFile("testFile");
        try {
            ensureExistingDirectory(parent);
        } finally {
            deleteQuiet(parent);
        }
    }

    @Test(expected = FileUtilsException.class)
    public void testEnsureExistingDirectory_withInaccessibleFile() {
        ensureExistingDirectory(new File(INACCESSIBLE_FILE, "test"));
    }

    @Test(expected = FileUtilsException.class)
    public void testEnsureExistingDirectory_whenFileCouldNotBeCreated() throws Exception {
        File file = mock(File.class);
        when(file.isFile()).thenReturn(false);
        when(file.isDirectory()).thenReturn(false);
        when(file.exists()).thenReturn(false);
        when(file.createNewFile()).thenReturn(false);

        ensureExistingDirectory(file);
    }

    @Test
    public void testRename() {
        File target = new File(EXAMPLE_FILE + "renamed");
        try {
            rename(EXAMPLE_FILE, target);

            assertFalse(EXAMPLE_FILE.exists());
            assertTrue(target.exists());
        } finally {
            deleteQuiet(target);
        }
    }

    @Test
    public void testRename_sourceNotFound() {
        rename(FILE_NOT_FOUND, new File("target"));
    }

    @Test(expected = FileUtilsException.class)
    public void testRename_whenCannotRename() {
        rename(EXAMPLE_FILE, INACCESSIBLE_FILE);
    }
}
