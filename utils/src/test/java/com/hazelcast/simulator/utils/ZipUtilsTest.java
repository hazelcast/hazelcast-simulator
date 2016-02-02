package com.hazelcast.simulator.utils;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.Collections;

import static com.hazelcast.simulator.utils.FileUtils.appendText;
import static com.hazelcast.simulator.utils.FileUtils.deleteQuiet;
import static com.hazelcast.simulator.utils.FileUtils.ensureExistingDirectory;
import static com.hazelcast.simulator.utils.FileUtils.ensureExistingFile;
import static com.hazelcast.simulator.utils.FileUtils.fileAsText;
import static com.hazelcast.simulator.utils.ReflectionUtils.invokePrivateConstructor;
import static com.hazelcast.simulator.utils.ZipUtils.unzip;
import static com.hazelcast.simulator.utils.ZipUtils.zip;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ZipUtilsTest {

    private File parent = ensureExistingDirectory("test");
    private File file = ensureExistingFile(parent, "zip-test.txt");
    private File dsStore = ensureExistingFile(parent, ".DS_Store");

    @Before
    public void setUp() {
        appendText("Lore ipsum", file);
    }

    @After
    public void tearDown() {
        deleteQuiet(file);
        deleteQuiet(dsStore);
        deleteQuiet(parent);
    }

    @Test
    public void testConstructor() throws Exception {
        invokePrivateConstructor(ZipUtils.class);
    }

    @Test
    public void testZipAndUnzip() throws Exception {
        byte[] zippedBytes = zip(Collections.singletonList(parent));

        deleteQuiet(file);
        assertFalse(file.exists());
        deleteQuiet(dsStore);
        assertFalse(dsStore.exists());

        unzip(zippedBytes, parent);

        assertTrue(file.exists());
        assertFalse(dsStore.exists());
        assertEquals("Lore ipsum", fileAsText(file));
    }
}
