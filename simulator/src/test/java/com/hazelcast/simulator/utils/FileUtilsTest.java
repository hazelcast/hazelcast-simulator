package com.hazelcast.simulator.utils;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.List;

import static com.hazelcast.simulator.utils.CliUtils.initOptionsWithHelp;
import static com.hazelcast.simulator.utils.CommonUtils.closeQuietly;
import static com.hazelcast.simulator.utils.FileUtils.USER_HOME;
import static com.hazelcast.simulator.utils.FileUtils.appendText;
import static com.hazelcast.simulator.utils.FileUtils.copy;
import static com.hazelcast.simulator.utils.FileUtils.copyFilesToDirectory;
import static com.hazelcast.simulator.utils.FileUtils.deleteQuiet;
import static com.hazelcast.simulator.utils.FileUtils.ensureExistingDirectory;
import static com.hazelcast.simulator.utils.FileUtils.ensureExistingFile;
import static com.hazelcast.simulator.utils.FileUtils.fileAsText;
import static com.hazelcast.simulator.utils.FileUtils.getFileAsTextFromWorkingDirOrBaseDir;
import static com.hazelcast.simulator.utils.FileUtils.getFileOrExit;
import static com.hazelcast.simulator.utils.FileUtils.getFilesFromClassPath;
import static com.hazelcast.simulator.utils.FileUtils.getResourceFile;
import static com.hazelcast.simulator.utils.FileUtils.getSimulatorHome;
import static com.hazelcast.simulator.utils.FileUtils.isValidFileName;
import static com.hazelcast.simulator.utils.FileUtils.newFile;
import static com.hazelcast.simulator.utils.FileUtils.rename;
import static com.hazelcast.simulator.utils.FileUtils.writeText;
import static com.hazelcast.simulator.utils.ReflectionUtils.invokePrivateConstructor;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class FileUtilsTest {
//
//    private static final File FILE_NOT_FOUND = new File("notFound");
//    private static final File INVALID_FILE = new File("\\//?:&|");
//    private static final File INACCESSIBLE_FILE = new File("/dev/null");
//
//    private static final File EXAMPLE_FILE = new File("fileUtilsTestFile");
//    private static final String EXAMPLE_CONTENT = "exampleContent";
//
//    private static final File TARGET_FILE = new File("target");
//
//    @Before
//    public void before() {
//        writeText(EXAMPLE_CONTENT, EXAMPLE_FILE);
//    }
//
//    @After
//    public void after() throws InterruptedException {
//        Thread.sleep(5000);
//        deleteQuiet(INVALID_FILE);
//        deleteQuiet(EXAMPLE_FILE);
//        deleteQuiet(TARGET_FILE);
//    }
//
//    @Test
//    public void testConstructor() throws Exception {
//        invokePrivateConstructor(FileUtils.class);
//    }
//
//    @Test
//    public void testIsValidFileName() {
//        assertTrue(isValidFileName("validFilename"));
//        assertTrue(isValidFileName("validFilename23"));
//        assertTrue(isValidFileName("42validFilename"));
//        assertTrue(isValidFileName("VALID_FILENAME"));
//
//        assertFalse(isValidFileName("INVALID FILENAME"));
//        assertFalse(isValidFileName("invalid$filename"));
//    }
//
//    @Test
//    public void testNewFile_withEmptyPath() {
//        File actual = newFile("");
//        assertEquals("", actual.getPath());
//    }
//
//    @Test
//    public void testNewFile_withUserHome() {
//        File actual = newFile("~");
//        assertEquals(USER_HOME, actual.getPath());
//    }
//
//    @Test
//    public void testNewFile_withUserHomeInString() {
//        File actual = newFile("~" + File.separator + "foobar");
//        assertEquals(USER_HOME + File.separator + "foobar", actual.getPath());
//    }
//
//    @Test
//    public void testNewFile_withParent() {
//        File parent = newFile("~");
//        File actual = newFile(parent, "foobar");
//        assertEquals(USER_HOME + File.separator + "foobar", actual.getPath());
//    }
//
//    @Test
//    public void testNewFile_manyItems() {
//        File actual = newFile("~", "foobar", "test");
//        assertEquals(USER_HOME + File.separator + "foobar" + File.separator + "test", actual.getPath());
//    }
//
//    @Test
//    public void testWriteText() {
//        String expected = "write test";
//        writeText(expected, TARGET_FILE);
//
//        String actual = fileAsText(TARGET_FILE);
//        assertEquals(expected, actual);
//    }
//
//    @Test(expected = NullPointerException.class)
//    public void testWriteText_withNullText() {
//        writeText(null, new File("ignored"));
//    }
//
//    @Test(expected = NullPointerException.class)
//    public void testWriteText_withNullFile() {
//        writeText("ignored", null);
//    }
//
//    @Test(expected = UncheckedIOException.class)
//    public void testWriteText_withInvalidFilename() {
//        writeText("ignored", INVALID_FILE);
//    }
//
//    @Test
//    public void testAppendText() {
//        String expected = "write test";
//        File file = appendText(expected, TARGET_FILE.getName());
//        appendText(expected, file);
//
//        String actual = fileAsText(file);
//        assertEquals(expected + expected, actual);
//    }
//
//    @Test(expected = NullPointerException.class)
//    public void testAppendText_withNullText() {
//        appendText(null, new File("ignored"));
//    }
//
//    @Test(expected = NullPointerException.class)
//    public void testAppendText_withNullFile() {
//        appendText("ignored", (File) null);
//    }
//
//    @Test(expected = UncheckedIOException.class)
//    public void testAppendText_withInvalidFilename() {
//        appendText("ignored", INVALID_FILE);
//    }
//
//    @Test
//    public void testGetResourceFile() {
//        String resourceFileContent = getResourceFile("fileUtilsTestFile.txt");
//        assertEquals("testContent", resourceFileContent);
//    }
//
//    @Test(expected = UncheckedIOException.class)
//    public void testGetResourceFile_fileNotFound() {
//        getResourceFile("notFound");
//    }
//
//    @Test
//    public void testFileAsText_withFileName() {
//        String actual = fileAsText(EXAMPLE_FILE.getAbsolutePath());
//        assertEquals(EXAMPLE_CONTENT, actual);
//    }
//
//    @Test(expected = UncheckedIOException.class)
//    public void testFileAsText_withInvalidFileName() {
//        fileAsText(INVALID_FILE.getAbsolutePath());
//    }
//
//    @Test
//    public void testFileAsText_withFile() {
//        String actual = fileAsText(EXAMPLE_FILE);
//        assertEquals(EXAMPLE_CONTENT, actual);
//    }
//
//    @Test(expected = UncheckedIOException.class)
//    public void testFileAsText_withInvalidFile() {
//        fileAsText(INVALID_FILE);
//    }
//
//    @Test
//    public void testDeleteQuiet_withFile() {
//        File file = ensureExistingFile("deleteQuietFile");
//        assertTrue(file.exists());
//
//        deleteQuiet(file.getAbsolutePath());
//
//        assertFalse(file.exists());
//    }
//
//    @Test
//    public void testDeleteQuiet_withDirectory() {
//        File parent = ensureExistingDirectory("deleteQuietDirectory");
//        File file = ensureExistingDirectory(parent, "deleteQuietFile");
//        assertTrue(parent.exists());
//        assertTrue(file.exists());
//
//        deleteQuiet(parent.getAbsolutePath());
//
//        assertFalse(parent.exists());
//        assertFalse(file.exists());
//    }
//
//    @Test
//    public void testDeleteQuiet_withInaccessibleFile() {
//        deleteQuiet(INACCESSIBLE_FILE);
//    }
//
//    @Test
//    public void testDeleteQuiet_withFileNotFound() {
//        deleteQuiet(FILE_NOT_FOUND);
//    }
//
//    @Test
//    public void testDeleteQuiet_withInvalidFile() {
//        deleteQuiet(INVALID_FILE);
//    }
//
//    @Test
//    public void testDeleteQuiet_withNullFile() {
//        deleteQuiet((File) null);
//    }
//
//    @Test
//    public void testEnsureExistingFile_withFileName() {
//        File file = ensureExistingFile(TARGET_FILE.getName());
//
//        assertTrue(file.exists());
//    }
//
//    @Test
//    public void testEnsureExistingFile_withParent() {
//        File parent = ensureExistingDirectory("parent");
//        try {
//            File file = ensureExistingFile(parent, "test");
//
//            assertTrue(file.exists());
//        } finally {
//            deleteQuiet(parent);
//        }
//    }
//
//    @Test
//    public void testEnsureExistingFile_withExistingFile() {
//        File file = null;
//        try {
//            file = ensureExistingFile("ensureExistingFileTest");
//            assertTrue(file.exists());
//
//            file = ensureExistingFile("ensureExistingFileTest");
//            assertTrue(file.exists());
//        } finally {
//            deleteQuiet(file);
//        }
//    }
//
//    @Test(expected = IllegalArgumentException.class)
//    public void testEnsureExistingFile_withExistingDirectory() {
//        ensureExistingDirectory(TARGET_FILE);
//
//        ensureExistingFile(TARGET_FILE);
//    }
//
//    @Test(expected = UncheckedIOException.class)
//    public void testEnsureExistingFile_withInaccessibleFile() {
//        ensureExistingFile(new File(INACCESSIBLE_FILE, "test"));
//    }
//
//    @Test(expected = UncheckedIOException.class)
//    public void testEnsureExistingFile_withInvalidFile() {
//        ensureExistingFile(INVALID_FILE);
//    }
//
//    @Test(expected = UncheckedIOException.class)
//    public void testEnsureExistingFile_whenFileCouldNotBeCreated() throws Exception {
//        File file = mock(File.class);
//        when(file.isFile()).thenReturn(false);
//        when(file.isDirectory()).thenReturn(false);
//        when(file.exists()).thenReturn(false);
//        when(file.createNewFile()).thenReturn(false);
//
//        ensureExistingFile(file);
//    }
//
//    @Test
//    public void testEnsureExistingDirectory_withFileName() {
//        File file = ensureExistingDirectory(TARGET_FILE.getName());
//
//        assertTrue(file.exists());
//    }
//
//    @Test
//    public void testEnsureExistingDirectory_withParent() {
//        File parent = ensureExistingDirectory("parent");
//        try {
//            File file = ensureExistingDirectory(parent, "child");
//
//            assertTrue(file.exists());
//        } finally {
//            deleteQuiet(parent);
//        }
//    }
//
//    @Test
//    public void testEnsureExistingDirectory_withExistingDirectory() {
//        File file = null;
//        try {
//            file = ensureExistingDirectory("ensureExistingDirectoryTest");
//            assertTrue(file.exists());
//
//            file = ensureExistingDirectory("ensureExistingDirectoryTest");
//            assertTrue(file.exists());
//        } finally {
//            deleteQuiet(file);
//        }
//    }
//
//    @Test(expected = IllegalArgumentException.class)
//    public void testEnsureExistingDirectory_withExistingFile() {
//        ensureExistingFile(TARGET_FILE);
//
//        ensureExistingDirectory(TARGET_FILE);
//    }
//
//    @Test(expected = UncheckedIOException.class)
//    public void testEnsureExistingDirectory_withInaccessibleFile() {
//        ensureExistingDirectory(new File(INACCESSIBLE_FILE, "test"));
//    }
//
//    @Test(expected = UncheckedIOException.class)
//    public void testEnsureExistingDirectory_whenFileCouldNotBeCreated() throws Exception {
//        File file = mock(File.class);
//        when(file.isFile()).thenReturn(false);
//        when(file.isDirectory()).thenReturn(false);
//        when(file.exists()).thenReturn(false);
//        when(file.createNewFile()).thenReturn(false);
//
//        ensureExistingDirectory(file);
//    }
//
//    @Test
//    public void testRename() {
//        rename(EXAMPLE_FILE, TARGET_FILE);
//
//        assertFalse(EXAMPLE_FILE.exists());
//        assertTrue(TARGET_FILE.exists());
//    }
//
//    @Test
//    public void testRename_sourceNotFound() {
//        rename(FILE_NOT_FOUND, new File("target"));
//    }
//
//    @Test(expected = UncheckedIOException.class)
//    public void testRename_whenCannotRename() {
//        rename(EXAMPLE_FILE, INACCESSIBLE_FILE);
//    }
//
//    @Test
//    public void testCopy_toOutputStream() {
//        OutputStream outputStream = new ByteArrayOutputStream();
//        copy(EXAMPLE_FILE, outputStream);
//
//        assertEquals(EXAMPLE_CONTENT, outputStream.toString());
//    }
//
//    @Test(expected = UncheckedIOException.class)
//    public void testCopy_toOutputStream_withClosedStream() throws Exception {
//        OutputStream outputStream = new FileOutputStream(TARGET_FILE.getName());
//        closeQuietly(outputStream);
//
//        copy(EXAMPLE_FILE, outputStream);
//    }
//
//    @Test
//    public void testGetSimulatorHome() {
//        File file = getSimulatorHome();
//
//        assertNotNull(file);
//    }
//
//    @Test
//    public void testGetFileOrExit() {
//        ensureExistingFile(TARGET_FILE);
//
//        OptionParser parser = new OptionParser();
//        OptionSpec<String> optionSpec = parser.accepts("fileName").withRequiredArg().ofType(String.class);
//
//        OptionSet options = initOptionsWithHelp(parser, new String[]{"--fileName", TARGET_FILE.getName()});
//
//        File file = getFileOrExit(optionSpec, options, "getFileTest");
//
//        assertEquals(TARGET_FILE, file);
//    }
//
//    @Test(expected = CommandLineExitException.class)
//    public void testGetFileOrExit_fileNotFound() {
//        OptionParser parser = new OptionParser();
//        OptionSpec<String> optionSpec = parser.accepts("fileName").withRequiredArg().ofType(String.class);
//
//        OptionSet options = initOptionsWithHelp(parser, new String[]{"--fileName", FILE_NOT_FOUND.getName()});
//
//        getFileOrExit(optionSpec, options, "getFileTest");
//    }
//
//    @Test
//    public void testGetFileAsTextFromWorkingDirOrBaseDir() {
//        String content = getFileAsTextFromWorkingDirOrBaseDir(null, EXAMPLE_FILE.getName(), "desc");
//
//        assertEquals(EXAMPLE_CONTENT, content);
//    }
//
//    @Test
//    public void testGetFileAsTextFromWorkingDirOrBaseDir_withFileFromBaseDir() {
//        String baseDir = System.getProperty("user.dir") + "/dist/src/main/dist";
//        String content = getFileAsTextFromWorkingDirOrBaseDir(new File(baseDir), "hazelcast.xml", "desc");
//
//        assertTrue(content.startsWith("<hazelcast"));
//    }
//
//    @Test(expected = UncheckedIOException.class)
//    public void testGetFileAsTextFromWorkingDirOrBaseDir_withFileNotFound() {
//        getFileAsTextFromWorkingDirOrBaseDir(null, FILE_NOT_FOUND.getName(), "desc");
//    }
//
//    @Test
//    public void testGetFilesFromClassPath() throws Exception {
//        ClassLoader cl = ClassLoader.getSystemClassLoader();
//        URL[] urls = ((URLClassLoader) cl).getURLs();
//
//        String delimiter = "";
//        StringBuilder classPath = new StringBuilder();
//        for (URL url : urls) {
//            classPath.append(delimiter).append(new File(url.toURI()));
//            delimiter = ";";
//        }
//
//        List<File> files = getFilesFromClassPath(classPath.toString());
//
//        assertFalse(files.isEmpty());
//    }
//
//    @Test
//    public void testGetFilesFromClassPath_whenNull_thenReturnEmptyList() {
//        List<File> files = getFilesFromClassPath(null);
//
//        assertTrue(files.isEmpty());
//    }
//
//    @Test
//    public void testGetFilesFromClassPath_withWildcards() {
//        ensureExistingDirectory(TARGET_FILE);
//        ensureExistingFile(TARGET_FILE, "test");
//
//        List<File> files = getFilesFromClassPath(TARGET_FILE.getName() + File.separator + "*");
//
//        assertFalse(files.isEmpty());
//    }
//
//    @Test(expected = UncheckedIOException.class)
//    public void testGetFilesFromClassPath_withWildcards_noDirectory() {
//        ensureExistingFile(TARGET_FILE);
//
//        getFilesFromClassPath(TARGET_FILE.getName() + File.separator + "*");
//    }
//
//    @Test(expected = UncheckedIOException.class)
//    public void testGetFilesFromClassPath_withFileNotFound() {
//        getFilesFromClassPath(FILE_NOT_FOUND.getName());
//    }
//
//    @Test
//    public void testCopyFilesToDirectory() {
//        ensureExistingDirectory(TARGET_FILE);
//
//        copyFilesToDirectory(new File[]{EXAMPLE_FILE}, TARGET_FILE);
//
//        assertTrue(EXAMPLE_FILE.exists());
//        assertTrue(TARGET_FILE.exists());
//    }
//
//    @Test(expected = UncheckedIOException.class)
//    public void testCopyFilesToDirectory_withInaccessibleTarget() {
//        copyFilesToDirectory(new File[]{EXAMPLE_FILE}, INACCESSIBLE_FILE);
//    }
}
