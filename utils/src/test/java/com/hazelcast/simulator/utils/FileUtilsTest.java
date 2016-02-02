package com.hazelcast.simulator.utils;

import org.junit.Test;

import java.io.File;

import static com.hazelcast.simulator.utils.FileUtils.USER_HOME;
import static com.hazelcast.simulator.utils.FileUtils.appendText;
import static com.hazelcast.simulator.utils.FileUtils.deleteQuiet;
import static com.hazelcast.simulator.utils.FileUtils.fileAsText;
import static com.hazelcast.simulator.utils.FileUtils.isValidFileName;
import static com.hazelcast.simulator.utils.FileUtils.newFile;
import static com.hazelcast.simulator.utils.FileUtils.writeText;
import static com.hazelcast.simulator.utils.ReflectionUtils.invokePrivateConstructor;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class FileUtilsTest {

    private static final File INVALID_FILE = new File(":\\//?&");

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
    public void testNewFileEmptyPath() {
        File actual = newFile("");
        assertEquals("", actual.getPath());
    }

    @Test
    public void testNewFileUserHome() {
        File actual = newFile("~");
        assertEquals(USER_HOME, actual.getPath());
    }

    @Test
    public void testNewFileUserHomeBased() {
        File actual = newFile("~" + File.separator + "foobar");
        assertEquals(USER_HOME + File.separator + "foobar", actual.getPath());
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
        File file = new File("testAppendText");
        try {
            String expected = "write test";
            appendText(expected, file);
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

    @Test(expected = FileUtilsException.class)
    public void testFileAsText_withInvalidFilename() {
        fileAsText(INVALID_FILE);
    }

    @Test
    public void testDeleteQuiet_withInvalidFilename() {
        deleteQuiet("/dev/null");
    }
}
