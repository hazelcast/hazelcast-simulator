package com.hazelcast.stabilizer.common;

import com.hazelcast.logging.ILogger;
import com.hazelcast.stabilizer.Utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import static com.hazelcast.stabilizer.Utils.newFile;
import static java.lang.String.format;

public class StabilizerProperties {
    private final static ILogger log = com.hazelcast.logging.Logger.getLogger(StabilizerProperties.class);

    private final Properties properties = new Properties();
    private File defaultPropsFile = newFile(Utils.getStablizerHome(), "conf", "stabilizer.properties");

    /**
     * Initialized the StabilizerProperties
     *
     * @param file the file to load the properties from. If the file is null, then first the stabilizer.properties
     *             in the working dir is checked and otherwise the stabilizer.properties in STABILIZER_HOME/conf is
     *             used.
     */
    public void init(File file) {
        load(defaultPropsFile);

        if (file == null) {
            //if no file is explicitly given, we look in the working directory
            File tmp = new File("stabilizer.properties");
            if (tmp.exists()) {
                file = tmp;
            }
        }

        if (file != null) {
            log.info(format("Loading stabilizer.properties: %s", file.getAbsolutePath()));
            load(file);
        } else {
            log.info(format("No specific stabilizer.properties provided, relying on default settings"));
        }
    }

    private void load(File file) {

        if (!file.exists()) {
            Utils.exitWithError(log, "Could not find stabilizer.properties file:  " + file.getAbsolutePath());
            return;
        }

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
