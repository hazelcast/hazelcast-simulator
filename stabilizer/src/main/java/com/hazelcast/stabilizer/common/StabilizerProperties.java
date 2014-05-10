package com.hazelcast.stabilizer.common;

import com.hazelcast.logging.ILogger;
import com.hazelcast.stabilizer.Utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class StabilizerProperties {
    private final static ILogger log = com.hazelcast.logging.Logger.getLogger(StabilizerProperties.class);

    private final Properties properties = new Properties();
    private File file;

    public void init(File file) {
        if (file == null) {
            //look in the working directory first
            file = new File("stabilizer.properties");
            if (!file.exists()) {
                //if not exist, then look in the conf directory.
                file = Utils.toFile(Utils.getStablizerHome(), "conf", "stabilizer.properties");
            }
        }

        if (!file.exists()) {
            Utils.exitWithError(log, "Could not find stabilizer.properties file:  " + file.getAbsolutePath());
        }

        load(file);
    }

    private void load(File file) {
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

    public File getFile() {
        return file;
    }

    public String get(String name) {
        return (String) properties.get(name);
    }

    public String get(String name, String defaultValue) {
        String value = System.getProperty(name);
        if (value != null) {
            return value;
        }

        value = (String) properties.get(name);
        if (value == null) {
            value = defaultValue;
        }
        return value;
    }
}
