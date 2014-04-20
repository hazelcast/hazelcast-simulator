/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast.stabilizer;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.Reader;
import java.io.StringWriter;
import java.io.Writer;
import java.net.Socket;
import java.net.URI;
import java.util.Deque;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import static java.lang.String.format;

public final class Utils {
    private final static ILogger log = Logger.getLogger(Utils.class);

    private static volatile String hostAddress;

    public static String getHostAddress() {
        if (hostAddress != null) {
            return hostAddress;
        }

        synchronized (Utils.class) {
            try {
                if(hostAddress!=null){
                    return hostAddress;
                }
                Socket s = new Socket("google.com", 80);
                hostAddress = s.getLocalAddress().getHostAddress();
                s.close();
                return hostAddress;
            } catch (IOException io) {
                throw new RuntimeException(io);
            }
        }
    }

    public static void writeObject(Object o, File file) {
        File tmpFile = new File(file.getParent(), file.getName() + ".tmp");

        try {
            final FileOutputStream fous = new FileOutputStream(tmpFile);
            try {
                ObjectOutput output = new ObjectOutputStream(fous);
                output.writeObject(o);
            } finally {
                Utils.closeQuietly(fous);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        if (!tmpFile.renameTo(file)) {
            throw new RuntimeException(format("Could not rename [%s] to [%s]", tmpFile.getAbsolutePath(), file.getAbsolutePath()));
        }
    }

    public static Object readObject(File file) {
        try {
            FileInputStream fis = new FileInputStream(file);
            try {
                ObjectInputStream in = new ObjectInputStream(fis);
                return in.readObject();
            } finally {
                Utils.closeQuietly(fis);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    public static void writeText(String text, File file) throws IOException {
        FileOutputStream stream = new FileOutputStream(file);
        try {
            Writer writer = new BufferedWriter(new OutputStreamWriter(stream));
            writer.write(text);
            writer.close();
        } finally {
            closeQuietly(stream);
        }
    }

    public static String asText(File file) {
        try {
            FileInputStream stream = new FileInputStream(file);
            try {
                Reader reader = new BufferedReader(new InputStreamReader(stream));
                StringBuilder builder = new StringBuilder();
                char[] buffer = new char[8192];
                int read;
                while ((read = reader.read(buffer, 0, buffer.length)) > 0) {
                    builder.append(buffer, 0, read);
                }
                return builder.toString();
            } finally {
                closeQuietly(stream);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    public static void delete(File f) throws IOException {
        if (!f.exists()) return;

        if (f.isDirectory()) {
            for (File c : f.listFiles())
                delete(c);
        }

        if (!f.delete())
            throw new FileNotFoundException("Failed to delete file: " + f);
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

    public static String getVersion() {
        String version = "";
        try {
            Properties p = new Properties();
            InputStream is = Utils.class.getResourceAsStream(
                    "/META-INF/maven/hazelcast-stabilizer/hazelcast-stabilizer/pom.properties");
            if (is != null) {
                p.load(is);
                return p.getProperty("version", "");
            }
        } catch (Exception e) {
            // ignore
        }
        return version;
    }

    public static byte[] zip(List<File> roots) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Deque<File> queue = new LinkedList<File>();
        ZipOutputStream zout = new ZipOutputStream(out);

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

                    log.finest("Zipping: " + file.getAbsolutePath());

                    if (file.isDirectory()) {
                        String name = base.relativize(file.toURI()).getPath();
                        name = name.endsWith("/") ? name : name + "/";

                        if (names.add(name)) {
                            zout.putNextEntry(new ZipEntry(name));
                        }

                        for (File kid : file.listFiles()) {
                            queue.push(kid);
                        }
                    } else {
                        String name = base.relativize(file.toURI()).getPath();
                        zout.putNextEntry(new ZipEntry(name));
                        copy(file, zout);
                        zout.closeEntry();
                    }
                }
            }
        } finally {
            zout.close();
        }

        return out.toByteArray();
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

    public static String throwableToString(Throwable t){
        StringWriter sw = new StringWriter();
        t.printStackTrace(new PrintWriter(sw));
        return sw.toString();
    }

    public static void unzip(byte[] content, final File destinationDir) throws IOException {
        byte[] buffer = new byte[1024];

        ZipInputStream zis = new ZipInputStream(new ByteArrayInputStream(content));
        ZipEntry zipEntry = zis.getNextEntry();

        while (zipEntry != null) {

            String fileName = zipEntry.getName();
            File file = new File(destinationDir + File.separator + fileName);

            log.finest("Unzipping: " + file.getAbsolutePath());


            if (zipEntry.isDirectory()) {
                file.mkdirs();
            } else {
                file.getParentFile().mkdirs();

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

    public static File getStablizerHome() {
        String home = System.getenv("STABILIZER_HOME");
        if (home == null) {
            return new File(System.getProperty("user.dir"));
        } else {
            return new File(home);
        }
    }

    public static void closeQuietly(Closeable c) {
        if (c == null) return;
        try {
            c.close();
        } catch (IOException ignore) {
        }
    }

    public static void sleepSeconds(int seconds) {
        try {
            Thread.sleep(seconds * 1000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public static void exitWithError(String msg) {
        System.out.printf(msg);
        System.exit(1);
    }

    private Utils() {
    }

    public static String secondsToHuman(int seconds) {
        StringBuffer sb = new StringBuffer();
        int time = seconds;

        int s = time % 60;

        time = time / 60;
        int m = time % 60;

        time = time / 60;
        int h = time % 24;

        time = time / 24;
        int days = time;

        sb.append(days).append(" days ");
        sb.append(h).append(" hours ");
        sb.append(m).append(" minutes ");
        sb.append(s).append(" seconds");

        return sb.toString();
    }

    public static Properties loadProperties(File file) {
        Properties properties = new Properties();
        final FileInputStream in;
        try {
            in = new FileInputStream(file);
        } catch (FileNotFoundException e) {
            //should not be thrown since it is already verified that the property file exist.
            throw new RuntimeException(e);
        }
        try {
            properties.load(in);
            return properties;
        } catch (IOException e) {
            throw new RuntimeException(format("Failed to load workout property file [%s]", file.getAbsolutePath()), e);
        } finally {
            Utils.closeQuietly(in);
        }
    }

    public static File getFile(OptionSpec<String> spec, OptionSet options, String desc) {
        File file = new File(options.valueOf(spec));
        if (!file.exists()) {
            exitWithError(format("%s [%s] does not exist\n", desc, file));
        }
        return file;
    }

    public static byte[] createUpload(String workerClassPath) throws IOException {
        if (workerClassPath == null) {
            return null;
        }

        String[] parts = workerClassPath.split(";");
        List<File> files = new LinkedList<File>();
        for (String filePath : parts) {
            File file = new File(filePath);

            if (file.getName().contains("*")) {
                File parent = file.getParentFile();
                if (!parent.isDirectory()) {
                    throw new IOException(format("Can't create upload, file [%s] is not a directory", parent));
                }

                String regex = file.getName().replace("*", "(.*)");
                for (File child : parent.listFiles()) {
                    if (child.getName().matches(regex)) {
                        files.add(child);
                    }
                }
            } else if (file.exists()) {
                files.add(file);
            } else {
                throw new IOException(format("Can't create upload, file [%s] doesn't exist", filePath));
            }
        }

        return zip(files);
    }
}
