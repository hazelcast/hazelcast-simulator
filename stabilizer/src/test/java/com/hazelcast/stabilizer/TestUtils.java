package com.hazelcast.stabilizer;

import java.io.File;
import java.io.IOException;

public class TestUtils {

    public static File writeToTempFile(String text) throws IOException {
        File file = File.createTempFile("test", "test");
        file.deleteOnExit();
        Utils.writeText(text, file);
        return file;
    }
}
