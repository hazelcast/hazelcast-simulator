package com.hazelcast.simulator.utils;

import org.junit.Test;

import java.io.File;

import static com.hazelcast.simulator.utils.FileUtils.USER_HOME;
import static com.hazelcast.simulator.utils.FileUtils.isValidFileName;
import static com.hazelcast.simulator.utils.FileUtils.newFile;
import static com.hazelcast.simulator.utils.ReflectionUtils.invokePrivateConstructor;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class FileUtilsTest {

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
}
