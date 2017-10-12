/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hazelcast.simulator.wizard;

import com.hazelcast.simulator.common.SimulatorProperties;
import com.hazelcast.simulator.utils.CommandLineExitException;
import com.hazelcast.simulator.utils.EmptyStatement;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.hazelcast.simulator.utils.CommonUtils.closeQuietly;
import static com.hazelcast.simulator.utils.CommonUtils.rethrow;
import static com.hazelcast.simulator.utils.FileUtils.ensureExistingFile;
import static com.hazelcast.simulator.utils.FileUtils.getResourceFile;
import static com.hazelcast.simulator.utils.FileUtils.writeText;
import static com.hazelcast.simulator.utils.NativeUtils.execute;
import static java.lang.String.format;

final class WizardUtils {

    private static final int FILE_EXTENSION_LENGTH = 4;

    private WizardUtils() {
    }

    static void copyResourceFile(File workDir, String targetName, String sourceName) {
        File runScript = ensureExistingFile(workDir, targetName);
        writeText(getResourceFile(sourceName), runScript);
        execute(format("chmod u+x %s", runScript.getAbsolutePath()));
    }

    static File getProfileFile(String directory) {
        File zshrcFile = new File(directory, ".zshrc");
        if (zshrcFile.isFile()) {
            return zshrcFile;
        }

        File bashrcFile = new File(directory, ".bashrc");
        if (bashrcFile.isFile()) {
            return bashrcFile;
        }

        File profileFile = new File(directory, ".profile");
        if (profileFile.isFile()) {
            return profileFile;
        }

        throw new CommandLineExitException("Could not find one of .zshrc, .bashrc or .profile files"
                + " under directory '" + directory + "`"
                + " Installation not supported on this system!");
    }

    static Properties getUserProperties() {
        FileInputStream inputStream = null;
        try {
            Properties properties = new Properties();

            File userPropertiesFile = new File(SimulatorProperties.PROPERTIES_FILE_NAME).getAbsoluteFile();
            if (!userPropertiesFile.exists()) {
                return properties;
            }

            inputStream = new FileInputStream(userPropertiesFile);
            properties.load(inputStream);

            return properties;
        } catch (IOException e) {
            throw rethrow(e);
        } finally {
            closeQuietly(inputStream);
        }
    }

    static boolean containsCommentedOutProperty(String propertiesString, String property) {
        Pattern pattern = Pattern.compile("^[\\t ]*#[\\t ]*" + property + "[\\t ]*=.*$", Pattern.MULTILINE);
        return pattern.matcher(propertiesString).find();
    }

    static String getCommentedOutProperty(String propertiesString, String property) {
        Pattern pattern = Pattern.compile("^[\\t ]*#[\\t ]*" + property + "[\\t ]*=[\\t ]*(.*)$", Pattern.MULTILINE);
        Matcher matcher = pattern.matcher(propertiesString);
        if (matcher.find()) {
            return matcher.group(1).trim();
        }
        return null;
    }

    static String getSimulatorPath() {
        return getJarDir(WizardUtils.class).getParentFile().getAbsolutePath();
    }

    /**
     * Compute the absolute file path to the JAR file.
     * <p>
     * Found in http://stackoverflow.com/a/20953376
     * The framework is based on http://stackoverflow.com/a/12733172/1614775
     * But that gets it right for only one of the four cases.
     *
     * @param clazz A class residing in the required JAR.
     * @return A File object for the directory in which the JAR file resides.
     * During testing with NetBeans, the result is ./build/classes/,
     * which is the directory containing what will be in the JAR.
     */
    static File getJarDir(Class clazz) {
        return getFileFromUrl(getUrl(clazz), clazz.getName());
    }

    static File getFileFromUrl(URL url, String className) {
        // convert to external form
        String extURL = url.toExternalForm();

        // prune for various cases
        if (extURL.endsWith(".jar")) {
            // from getCodeSource
            extURL = extURL.substring(0, extURL.lastIndexOf('/'));
        } else {
            // from getResource
            String suffix = "/" + className.replace(".", "/") + ".class";
            extURL = extURL.replace(suffix, "");
            if (extURL.startsWith("jar:") && extURL.endsWith(".jar!")) {
                extURL = extURL.substring(FILE_EXTENSION_LENGTH, extURL.lastIndexOf('/'));
            }
        }

        // convert back to URL
        try {
            url = new URL(extURL);
        } catch (MalformedURLException e) {
            // leave url unchanged; probably does not happen
            EmptyStatement.ignore(e);
        }

        // convert URL to File
        try {
            return new File(url.toURI());
        } catch (Exception ignored) {
            return new File(url.getPath());
        }
    }

    private static URL getUrl(Class clazz) {
        try {
            return clazz.getProtectionDomain().getCodeSource().getLocation();
            // URL is in one of two forms
            //        ./build/classes/    NetBeans test
            //        jars/JarName.jar    from a JAR
        } catch (SecurityException e) {
            return clazz.getResource(clazz.getSimpleName() + ".class");
            // URL is in one of two forms, both ending "/com/hazelcast/simulator/wizard/WizardUtils.class"
            //          file:/U:/Simulator/java/simulator/wizard/build/classes
            //          jar:file:/U:/Simulator/java/simulator/wizard/dist/Wizard.jar!
        }
    }
}
