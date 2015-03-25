package com.hazelcast.simulator.visualiser.utils;

import java.io.File;

public final class FileUtils {

    private FileUtils() {
    }

    public static String getFileName(File file) {
        return removeExtension(file.getName());
    }

    public static String removeExtension(String name) {
        int dotPos = name.lastIndexOf('.');
        if (dotPos != -1) {
            name = name.substring(0, dotPos);
        }
        return name;
    }
}
