/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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


import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.commons.lang3.text.StrSubstitutor;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Pattern;

import static com.hazelcast.simulator.utils.CommonUtils.closeQuietly;
import static com.hazelcast.simulator.utils.EmptyStatement.ignore;
import static com.hazelcast.simulator.utils.Preconditions.checkNotNull;
import static java.lang.String.format;
import static java.nio.charset.Charset.forName;
import static java.util.Collections.emptyList;

@SuppressFBWarnings("DM_DEFAULT_ENCODING")
@SuppressWarnings("checkstyle:methodcount")
public final class FileUtils {

    public static final String USER_HOME = System.getProperty("user.home");

    private static final int READ_BUFFER_SIZE = 8192;
    private static final int COPY_BUFFER_SIZE = 1024;

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

    public static void writeText(String text, File file) {
        checkNotNull(text, "Text can't be null");
        checkNotNull(file, "File can't be null");

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
            throw new UncheckedIOException(e);
        } finally {
            closeQuietly(writer);
            closeQuietly(streamWriter);
            closeQuietly(stream);
        }
    }

    public static File appendText(String text, String fileName) {
        File file = new File(fileName);
        appendText(text, file);
        return file;
    }

    public static void appendText(String text, File file) {
        checkNotNull(text, "Text can't be null");
        checkNotNull(file, "File can't be null");

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
            throw new UncheckedIOException("Could not append text", e);
        } finally {
            closeQuietly(writer);
            closeQuietly(streamWriter);
            closeQuietly(stream);
        }
    }

    public static String toTextFromResourceFile(String fileName) {
        ClassLoader classLoader = FileUtils.class.getClassLoader();
        InputStream stream = classLoader.getResourceAsStream(fileName);
        try {
            return toTextFromStream(stream);
        } finally {
            closeQuietly(stream);
        }
    }

    public static String fileAsText(String fileName) {
        return fileAsText(new File(fileName));
    }

    public static String fileAsText(File file) {
        FileInputStream stream = null;
        try {
            stream = new FileInputStream(file);
            return toTextFromStream(stream);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } finally {
            closeQuietly(stream);
        }
    }

    private static String toTextFromStream(InputStream inputStream) {
        InputStreamReader streamReader = null;
        Reader reader = null;
        try {
            streamReader = new InputStreamReader(inputStream, forName("UTF-8"));
            reader = new BufferedReader(streamReader);

            StringBuilder builder = new StringBuilder();
            char[] buffer = new char[READ_BUFFER_SIZE];
            int read;
            while ((read = reader.read(buffer, 0, buffer.length)) > 0) {
                builder.append(buffer, 0, read);
            }
            return builder.toString();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } finally {
            closeQuietly(reader, streamReader);
        }
    }

    public static void deleteQuiet(File file) {
        if (file == null) {
            return;
        }
        try {
            delete(file);
        } catch (Exception ignored) {
            ignore(ignored);
        }
    }

    public static void delete(File file) {
        if (!file.exists()) {
            return;
        }

        if (file.isDirectory()) {
            File[] files = file.listFiles();
            if (files != null) {
                for (File fileInDirectory : files) {
                    delete(fileInDirectory);
                }
            }
        }

        if (!file.delete()) {
            throw new UncheckedIOException("Failed to delete file: " + file);
        }
    }

    public static File ensureExistingFile(String fileName) {
        return ensureExistingFile(new File(fileName));
    }

    public static File ensureExistingFile(File parent, String fileName) {
        return ensureExistingFile(new File(parent, fileName));
    }

    public static File ensureExistingFile(File file) {
        if (file.isFile()) {
            return file;
        }

        if (file.isDirectory()) {
            throw new IllegalArgumentException(format("File [%s] is a directory", file.getAbsolutePath()));
        }

        if (!file.exists()) {
            try {
                if (!file.createNewFile()) {
                    throw new UncheckedIOException("Could not create file: " + file.getAbsolutePath());
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        return file;
    }

    public static File ensureExistingDirectory(String dirName) {
        return ensureExistingDirectory(new File(dirName));
    }

    public static File ensureExistingDirectory(File parent, String dirName) {
        return ensureExistingDirectory(new File(parent, dirName));
    }

    public static File ensureExistingDirectory(File dir) {
        if (dir.isDirectory()) {
            return dir;
        }

        if (dir.isFile()) {
            throw new IllegalArgumentException(format("File [%s] is not a directory", dir.getAbsolutePath()));
        }

        // we don't care about the result because multiple threads are allowed to call this method concurrently
        // and therefore mkdirs() can return false if the directory has been created by another thread
        //noinspection ResultOfMethodCallIgnored
        dir.mkdirs();

        // we just need to make sure the directory is created
        if (!dir.exists()) {
            throw new UncheckedIOException("Could not create directory: " + dir.getAbsolutePath());
        }

        return dir;
    }

    public static File ensureNewDirectory(File dir) {
        if (dir.exists()) {
            throw new UncheckedIOException(format("Directory [%s] already exists", dir.getAbsoluteFile()));
        }
        if (!dir.mkdirs()) {
            throw new UncheckedIOException("Could not create directory [%s]" + dir.getAbsolutePath());
        }
        return dir;
    }

    public static void rename(File source, File target) {
        if (!source.exists()) {
            return;
        }
        if (!source.renameTo(target)) {
            throw new UncheckedIOException(format("Could not rename [%s] to [%s]",
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
            throw new UncheckedIOException(e);
        } finally {
            closeQuietly(inputStream);
        }
    }

    public static void copy(File from, File to) {
        FileOutputStream out;
        try {
            out = new FileOutputStream(to);
        } catch (FileNotFoundException e) {
            throw new UncheckedIOException(e);
        }
        try {
            copy(from, out);
        } finally {
            closeQuietly(out);
        }

        if (from.canExecute()) {
            to.setExecutable(true);
        }
    }

    public static File getUserDir() {
        String userDirTest = System.getProperty("user.dir.test");
        return userDirTest == null ? new File(System.getProperty("user.dir")) : new File(userDirTest);
    }

    public static String getUserHomePath() {
        String userDirTest = System.getProperty("user.dir.test");
        return userDirTest == null ? System.getProperty("user.home") : new File(userDirTest).getAbsolutePath();
    }

    public static File getSimulatorHome() {
        String home = System.getProperty("SIMULATOR_HOME");
        if (home != null) {
            return new File(home);
        }

        home = System.getenv("SIMULATOR_HOME");
        return home != null ? new File(home) : getUserDir();
    }

    public static List<File> getFilesFromClassPath(String classpath) {
        if (classpath == null) {
            return emptyList();
        }

        List<File> files = new LinkedList<File>();
        for (String filePath : classpath.split(";")) {
            File file = new File(filePath);

            if (file.getName().contains("*")) {
                File parent = file.getParentFile();
                if (parent == null || !parent.isDirectory()) {
                    throw new UncheckedIOException(format("Could not find matching files for wildcard classpath [%s]", filePath));
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
                throw new UncheckedIOException(format("Cannot convert classpath to java.io.File. [%s] doesn't exist", filePath));
            }
        }

        return files;
    }

    /**
     * Copies a directory recursively.
     *
     * @param src    the src directory
     * @param target the target directory
     */
    public static void copyDirectory(File src, File target) {
        checkNotNull(src, "src can't be null");
        checkNotNull(target, "target can't be null");

        File[] files = src.listFiles();
        if (files == null) {
            return;
        }

        for (File srcFile : files) {
            if (srcFile.isDirectory()) {
                File targetChild = new File(target, srcFile.getName());
                ensureExistingDirectory(targetChild);
                copyDirectory(srcFile, targetChild);
            } else {
                copyFileToDirectory(srcFile, target);
            }
        }
    }

    public static void copyFileToDirectory(File sourceFile, File targetDirectory) {
        File targetFile = newFile(targetDirectory, sourceFile.getName());
        copy(sourceFile, targetFile);
    }

    public static File getConfigurationFile(String filename) {
        File file = new File(getUserDir(), filename).getAbsoluteFile();
        return file.exists() ? file : newFile(getSimulatorHome(), "conf", filename).getAbsoluteFile();
    }
}
