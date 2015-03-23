package com.hazelcast.simulator.utils;

import com.google.common.io.Files;
import com.hazelcast.simulator.coordinator.Coordinator;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.apache.commons.lang3.text.StrSubstitutor;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.net.URI;
import java.net.URL;
import java.net.URLConnection;
import java.util.Collections;
import java.util.Deque;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import static com.hazelcast.simulator.utils.CommonUtils.closeQuietly;
import static com.hazelcast.simulator.utils.CommonUtils.exitWithError;
import static java.lang.String.format;

public class FileUtils {

    private static final Pattern VALID_FILE_NAME_PATTERN = Pattern.compile("^[a-zA-Z0-9-_]+$");
    private static final String USER_HOME = System.getProperty("user.home");
    private static final Logger LOGGER = Logger.getLogger(FileUtils.class);

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

    public static String getText(String url) throws IOException {
        URL website = new URL(url);
        URLConnection connection = website.openConnection();

        BufferedReader in = null;
        try {
            in = new BufferedReader(new InputStreamReader(connection.getInputStream()));

            StringBuilder response = new StringBuilder();
            String inputLine;

            while ((inputLine = in.readLine()) != null) {
                response.append(inputLine);
            }

            in.close();

            return response.toString();
        } finally {
            closeQuietly(in);
        }
    }

    public static File newFile(File file, String... items) {
        for (String item : items) {
            file = new File(file, item);
        }
        return file;
    }

    public static File newFile(String... items) {
        File file = newFile(items[0]);
        for (int k = 1; k < items.length; k++) {
            file = new File(file, items[k]);
        }
        return file;
    }

    public static void writeObject(Object o, File file) {
        File tmpFile = new File(file.getParent(), file.getName() + ".tmp");

        FileOutputStream fileOutputStream = null;
        try {
            fileOutputStream = new FileOutputStream(tmpFile);
            ObjectOutputStream objectOutputStream = null;
            try {
                objectOutputStream = new ObjectOutputStream(fileOutputStream);
                objectOutputStream.writeObject(o);
            } finally {
                closeQuietly(objectOutputStream);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            closeQuietly(fileOutputStream);
        }

        if (!tmpFile.renameTo(file)) {
            throw new RuntimeException(
                    format("Could not rename [%s] to [%s]", tmpFile.getAbsolutePath(), file.getAbsolutePath()));
        }
    }

    @SuppressWarnings("unchecked")
    public static <E> E readObject(File file) {
        FileInputStream fileInputStream = null;
        try {
            fileInputStream = new FileInputStream(file);
            ObjectInputStream objectInputStream = null;
            try {
                objectInputStream = new ObjectInputStream(fileInputStream);
                return (E) objectInputStream.readObject();
            } finally {
                closeQuietly(objectInputStream);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        } finally {
            closeQuietly(fileInputStream);
        }
    }

    public static void writeText(String text, File file) {
        if (text == null) {
            throw new NullPointerException("text can't be null");
        }

        if (file == null) {
            throw new NullPointerException("file can't be null");
        }

        try {
            FileOutputStream stream = new FileOutputStream(file);
            try {
                Writer writer = new BufferedWriter(new OutputStreamWriter(stream));
                writer.write(text);
                writer.close();
            } finally {
                closeQuietly(stream);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void appendText(String text, String file) {
        appendText(text, new File(file));
    }

    public static void appendText(String text, File file) {
        if (text == null) {
            throw new NullPointerException("Text can't be null");
        }

        if (file == null) {
            throw new NullPointerException("File can't be null");
        }

        try {
            FileOutputStream stream = new FileOutputStream(file, true);
            try {
                Writer writer = new BufferedWriter(new OutputStreamWriter(stream));
                writer.write(text);
                writer.close();
            } finally {
                closeQuietly(stream);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static String fileAsText(String filePath) {
        return fileAsText(new File(filePath));
    }

    public static String[] fileAsLines(File file) {
        return fileAsText(file).split("\n");
    }

    public static String fileAsText(File file) {
        FileInputStream stream = null;
        try {
            stream = new FileInputStream(file);
            Reader reader = null;
            try {
                reader = new BufferedReader(new InputStreamReader(stream));
                StringBuilder builder = new StringBuilder();
                char[] buffer = new char[8192];
                int read;
                while ((read = reader.read(buffer, 0, buffer.length)) > 0) {
                    builder.append(buffer, 0, read);
                }
                return builder.toString();
            } finally {
                closeQuietly(reader);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            closeQuietly(stream);
        }
    }

    public static void delete(File file) throws IOException {
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
            throw new FileNotFoundException("Failed to delete file: " + file);
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
                    throw new RuntimeException("Could not create directory: " + file.getAbsolutePath());
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
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
            throw new RuntimeException("Could not create directory: " + dir.getAbsolutePath());
        }
    }

    public static byte[] zip(List<File> roots) throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        ZipOutputStream zipOutputStream = new ZipOutputStream(outputStream);
        Deque<File> queue = new LinkedList<File>();

        Set<String> names = new HashSet<String>();

        try {
            for (File root : roots) {
                URI base = root.isDirectory() ? root.toURI() : root.getParentFile().toURI();
                queue.push(root);
                while (!queue.isEmpty()) {
                    File file = queue.pop();
                    if (file.getName().equals(".DS_Store")) {
                        continue;
                    }

                    //LOGGER.finest("Zipping: " + file.getAbsolutePath());
                    if (file.isDirectory()) {
                        String name = base.relativize(file.toURI()).getPath();
                        name = name.endsWith("/") ? name : name + "/";

                        if (names.add(name)) {
                            zipOutputStream.putNextEntry(new ZipEntry(name));
                        }

                        File[] files = file.listFiles();
                        if (files != null) {
                            for (File kid : files) {
                                queue.push(kid);
                            }
                        }
                    } else {
                        String name = base.relativize(file.toURI()).getPath();
                        zipOutputStream.putNextEntry(new ZipEntry(name));
                        copy(file, zipOutputStream);
                        zipOutputStream.closeEntry();
                    }
                }
            }
        } finally {
            zipOutputStream.close();
        }

        return outputStream.toByteArray();
    }

    private static void copy(InputStream in, OutputStream out) throws IOException {
        byte[] buffer = new byte[1024];
        while (true) {
            int readCount = in.read(buffer);
            if (readCount < 0) {
                break;
            }
            out.write(buffer, 0, readCount);
        }
    }

    private static void copy(File file, OutputStream out) throws IOException {
        InputStream in = new FileInputStream(file);
        try {
            copy(in, out);
        } finally {
            in.close();
        }
    }

    public static void unzip(byte[] content, final File destinationDir) throws IOException {
        byte[] buffer = new byte[1024];

        ZipInputStream zis = new ZipInputStream(new ByteArrayInputStream(content));
        ZipEntry zipEntry = zis.getNextEntry();

        while (zipEntry != null) {
            String fileName = zipEntry.getName();
            File file = new File(destinationDir + File.separator + fileName);

            //LOGGER.finest("Unzipping: " + file.getAbsolutePath());

            if (zipEntry.isDirectory()) {
                ensureExistingDirectory(file);
            } else {
                ensureExistingDirectory(file.getParentFile());

                FileOutputStream fos = new FileOutputStream(file);
                try {
                    int len;
                    while ((len = zis.read(buffer)) > 0) {
                        fos.write(buffer, 0, len);
                    }
                } finally {
                    closeQuietly(fos);
                }
            }

            zipEntry = zis.getNextEntry();
        }

        zis.closeEntry();
        zis.close();
    }

    public static File getSimulatorHome() {
        String home = System.getenv("SIMULATOR_HOME");
        if (home == null) {
            return new File(System.getProperty("user.dir"));
        } else {
            return new File(home);
        }
    }

    public static Properties loadProperties(File file) {
        Properties properties = new Properties();
        final FileInputStream in;
        try {
            in = new FileInputStream(file);
        } catch (FileNotFoundException e) {
            // should not be thrown since it is already verified that the property file exist
            throw new RuntimeException(e);
        }
        try {
            properties.load(in);
            return properties;
        } catch (IOException e) {
            throw new RuntimeException(format("Failed to load testsuite property file [%s]", file.getAbsolutePath()), e);
        } finally {
            closeQuietly(in);
        }
    }

    public static File getFile(OptionSpec<String> spec, OptionSet options, String desc) {
        File file = newFile(options.valueOf(spec));
        if (!file.exists()) {
            exitWithError(LOGGER, format("%s [%s] does not exist%n", desc, file));
        }
        return file;
    }

    public static String getFileAsTextFromWorkingDirOrSimulatorHome(String fileName, String desc) {
        File file = newFile(fileName);
        if (!file.exists()) {
            file = newFile(Coordinator.SIMULATOR_HOME + File.separator + "conf" + File.separator + fileName);
        }
        if (!file.exists()) {
            exitWithError(LOGGER, format("%s [%s] does not exist%n", desc, file.getAbsolutePath()));
        }
        LOGGER.info("Loading " + desc + ": " + file.getAbsolutePath());

        return fileAsText(file);
    }

    @SuppressWarnings("unchecked")
    public static List<File> getFilesFromClassPath(String classpath) throws IOException {
        if (classpath == null) {
            return Collections.EMPTY_LIST;
        }

        List<File> files = new LinkedList<File>();
        for (String filePath : classpath.split(";")) {
            File file = new File(filePath);

            if (file.getName().contains("*")) {
                File parent = file.getParentFile();
                if (!parent.isDirectory()) {
                    throw new IOException(format("Cannot convert classpath to java.io.File. [%s] is not a directory", parent));
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
                exitWithError(LOGGER, format("Cannot convert classpath to java.io.File. [%s] doesn't exist", filePath));
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
            String errorMessage = format("Error while copying file from %s to %s", sourceFile.getAbsolutePath(),
                    targetFile.getAbsolutePath());
            exitWithError(LOGGER, errorMessage, e);
        }
    }

    private FileUtils() {
    }
}
