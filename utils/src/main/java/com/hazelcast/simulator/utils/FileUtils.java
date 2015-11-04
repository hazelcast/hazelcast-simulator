/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast.simulator.utils;

import com.google.common.io.Files;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.apache.commons.lang3.text.StrSubstitutor;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.URL;
import java.net.URLConnection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Pattern;

import static com.hazelcast.simulator.utils.CommonUtils.closeQuietly;
import static java.lang.String.format;

@SuppressFBWarnings({"DM_DEFAULT_ENCODING"})
public final class FileUtils {

    public static final String USER_HOME = System.getProperty("user.home");

    private static final int READ_BUFFER_SIZE = 8192;
    private static final int COPY_BUFFER_SIZE = 1024;

    private static final Logger LOGGER = Logger.getLogger(FileUtils.class);
    private static final Pattern VALID_FILE_NAME_PATTERN = Pattern.compile("^[a-zA-Z0-9-_]+$");

    private FileUtils() {
    }

    public static boolean isValidFileName(String fileName) {
        return VALID_FILE_NAME_PATTERN.matcher(fileName).matches();
    }

    public static File newFile(String path) {
        path = path.trim();
        if (path.equals("~")) {
            path = USER_HOME;
        } else if (path.startsWith("~" + File.separator)) {
            path = USER_HOME + path.substring(1);
        }
        path = new StrSubstitutor().replace(path);

        return new File(path);
    }

    public static File newFile(File file, String... items) {
        for (String item : items) {
            file = new File(file, item);
        }
        return file;
    }

    public static File newFile(String... items) {
        File file = newFile(items[0]);
        for (int i = 1; i < items.length; i++) {
            file = new File(file, items[i]);
        }
        return file;
    }

    public static void writeObject(Object o, File file) {
        File tmpFile = new File(file.getParent(), file.getName() + ".tmp");

        FileOutputStream stream = null;
        ObjectOutputStream outputStream = null;
        try {
            stream = new FileOutputStream(tmpFile);
            outputStream = new ObjectOutputStream(stream);
            outputStream.writeObject(o);
        } catch (IOException e) {
            throw new FileUtilsException(e);
        } finally {
            closeQuietly(outputStream);
            closeQuietly(stream);
        }

        rename(tmpFile, file);
    }

    @SuppressWarnings("unchecked")
    public static <E> E readObject(File file) {
        FileInputStream stream = null;
        ObjectInputStream inputStream = null;
        try {
            stream = new FileInputStream(file);
            inputStream = new ObjectInputStream(stream);
            return (E) inputStream.readObject();
        } catch (IOException e) {
            throw new FileUtilsException(e);
        } catch (ClassNotFoundException e) {
            throw new FileUtilsException(e);
        } finally {
            closeQuietly(inputStream);
            closeQuietly(stream);
        }
    }

    public static void writeText(String text, File file) {
        if (text == null) {
            throw new NullPointerException("text can't be null");
        }

        if (file == null) {
            throw new NullPointerException("file can't be null");
        }

        FileOutputStream stream = null;
        OutputStreamWriter streamWriter = null;
        BufferedWriter writer = null;
        try {
            stream = new FileOutputStream(file);
            streamWriter = new OutputStreamWriter(stream);
            writer = new BufferedWriter(streamWriter);
            writer.write(text);
            writer.close();
        } catch (IOException e) {
            throw new FileUtilsException(e);
        } finally {
            closeQuietly(writer);
            closeQuietly(streamWriter);
            closeQuietly(stream);
        }
    }

    public static void appendText(String text, String fileName) {
        appendText(text, new File(fileName));
    }

    public static void appendText(String text, File file) {
        if (text == null) {
            throw new NullPointerException("Text can't be null");
        }
        if (file == null) {
            throw new NullPointerException("File can't be null");
        }

        FileOutputStream stream = null;
        OutputStreamWriter streamWriter = null;
        BufferedWriter writer = null;
        try {
            stream = new FileOutputStream(file, true);
            streamWriter = new OutputStreamWriter(stream);
            writer = new BufferedWriter(streamWriter);
            writer.append(text);
            writer.close();
        } catch (IOException e) {
            throw new FileUtilsException("Could not append text", e);
        } finally {
            closeQuietly(writer);
            closeQuietly(streamWriter);
            closeQuietly(stream);
        }
    }

    public static String getText(String url) {
        try {
            URL website = new URL(url);
            URLConnection connection = website.openConnection();

            InputStreamReader streamReader = null;
            BufferedReader reader = null;
            try {
                streamReader = new InputStreamReader(connection.getInputStream());
                reader = new BufferedReader(streamReader);

                StringBuilder response = new StringBuilder();
                String inputLine;
                while ((inputLine = reader.readLine()) != null) {
                    response.append(inputLine);
                }
                return response.toString();
            } finally {
                closeQuietly(reader);
                closeQuietly(streamReader);
            }
        } catch (IOException e) {
            throw new FileUtilsException(e);
        }
    }

    public static String fileAsText(String filePath) {
        return fileAsText(new File(filePath));
    }

    public static String fileAsText(File file) {
        FileInputStream stream = null;
        InputStreamReader streamReader = null;
        BufferedReader reader = null;
        try {
            stream = new FileInputStream(file);
            streamReader = new InputStreamReader(stream);
            reader = new BufferedReader(streamReader);

            StringBuilder builder = new StringBuilder();
            char[] buffer = new char[READ_BUFFER_SIZE];
            int read;
            while ((read = reader.read(buffer, 0, buffer.length)) > 0) {
                builder.append(buffer, 0, read);
            }
            return builder.toString();
        } catch (IOException e) {
            throw new FileUtilsException(e);
        } finally {
            closeQuietly(reader);
            closeQuietly(streamReader);
            closeQuietly(stream);
        }
    }

    public static void deleteQuiet(File file) {
        try {
            delete(file);
        } catch (Exception ignored) {
            EmptyStatement.ignore(ignored);
        }
    }

    public static void delete(File file) {
        if (!file.exists()) {
            return;
        }

        if (file.isDirectory()) {
            File[] files = file.listFiles();
            if (files == null) {
                return;
            }
            for (File fileInDirectory : files) {
                delete(fileInDirectory);
            }
        }

        if (!file.delete()) {
            throw new FileUtilsException("Failed to delete file: " + file);
        }
    }

    public static void ensureExistingFile(File file) {
        if (file.isFile()) {
            return;
        }

        if (file.isDirectory()) {
            throw new IllegalArgumentException(format("File [%s] is a directory", file.getAbsolutePath()));
        }

        if (!file.exists()) {
            try {
                if (!file.createNewFile()) {
                    throw new FileUtilsException("Could not create file: " + file.getAbsolutePath());
                }
            } catch (IOException e) {
                throw new FileUtilsException(e);
            }
        }
    }

    public static void ensureExistingDirectory(File dir) {
        if (dir.isDirectory()) {
            return;
        }

        if (dir.isFile()) {
            throw new IllegalArgumentException(format("File [%s] is not a directory", dir.getAbsolutePath()));
        }

        if (!dir.mkdirs()) {
            throw new FileUtilsException("Could not create directory: " + dir.getAbsolutePath());
        }
    }

    public static void rename(File source, File target) {
        if (!source.exists()) {
            return;
        }
        if (!source.renameTo(target)) {
            throw new FileUtilsException(format("Could not rename [%s] to [%s]",
                    source.getAbsolutePath(), target.getAbsolutePath()));
        }
    }

    public static void copy(File file, OutputStream outputStream) {
        InputStream inputStream = null;
        try {
            inputStream = new FileInputStream(file);

            byte[] buffer = new byte[COPY_BUFFER_SIZE];
            while (true) {
                int readCount = inputStream.read(buffer);
                if (readCount < 0) {
                    break;
                }
                outputStream.write(buffer, 0, readCount);
            }
        } catch (IOException e) {
            throw new FileUtilsException(e);
        } finally {
            closeQuietly(inputStream);
        }
    }

    public static File getSimulatorHome() {
        String home = System.getenv("SIMULATOR_HOME");
        if (home == null) {
            return new File(System.getProperty("user.dir"));
        } else {
            return new File(home);
        }
    }

    public static File getFile(OptionSpec<String> spec, OptionSet options, String desc) {
        File file = newFile(options.valueOf(spec));
        if (!file.exists()) {
            throw new FileUtilsException(format("%s [%s] does not exist", desc, file));
        }
        return file;
    }

    public static String getFileAsTextFromWorkingDirOrBaseDir(File baseDir, String fileName, String desc) {
        File file = newFile(fileName);
        if (!file.exists()) {
            file = newFile(baseDir + File.separator + "conf" + File.separator + fileName);
        }
        if (!file.exists()) {
            throw new FileUtilsException(format("%s [%s] does not exist", desc, file.getAbsolutePath()));
        }
        LOGGER.info("Loading " + desc + ": " + file.getAbsolutePath());

        return fileAsText(file);
    }

    @SuppressWarnings("unchecked")
    public static List<File> getFilesFromClassPath(String classpath) {
        if (classpath == null) {
            return Collections.EMPTY_LIST;
        }

        List<File> files = new LinkedList<File>();
        for (String filePath : classpath.split(";")) {
            File file = new File(filePath);

            if (file.getName().contains("*")) {
                File parent = file.getParentFile();
                if (parent == null || !parent.isDirectory()) {
                    throw new FileUtilsException(format("Could not find matching files for wildcard classpath %s",
                            file.getName()));
                }

                String regex = file.getName().replace("*", "(.*)");
                File[] parentFiles = parent.listFiles();
                if (parentFiles != null) {
                    for (File child : parentFiles) {
                        if (child.getName().matches(regex)) {
                            files.add(child);
                        }
                    }
                }
            } else if (file.exists()) {
                files.add(file);
            } else {
                throw new FileUtilsException(format("Cannot convert classpath to java.io.File. [%s] doesn't exist", filePath));
            }
        }

        return files;
    }

    public static void copyFilesToDirectory(File[] sourceFiles, File targetDirectory) {
        for (File sourceFile : sourceFiles) {
            copyFileToDirectory(sourceFile, targetDirectory);
        }
    }

    public static void copyFileToDirectory(File sourceFile, File targetDirectory) {
        File targetFile = newFile(targetDirectory, sourceFile.getName());
        try {
            Files.copy(sourceFile, targetFile);
        } catch (IOException e) {
            throw new FileUtilsException(format("Error while copying file from %s to %s", sourceFile.getAbsolutePath(),
                    targetFile.getAbsolutePath()), e);
        }
    }
}
