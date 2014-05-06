package com.hazelcast.stabilizer.common;

import com.hazelcast.stabilizer.Utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class StabilizerProperties {

    private final Properties properties = new Properties();
    private File file;

    public void load(File file) {
        this.file = file;
        try {
            FileInputStream inputStream = new FileInputStream(file);
            try {
                properties.load(inputStream);
            } catch (IOException e) {
                Utils.closeQuietly(inputStream);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public File getFile(){
        return file;
    }

    public String get(String name) {
        return (String) properties.get(name);
    }

    public String get(String name, String defaultValue) {
        String value = (String) properties.get(name);
        if (value == null) {
            value = defaultValue;
        }
        return value;
    }
}
