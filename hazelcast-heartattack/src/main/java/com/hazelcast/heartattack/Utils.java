package com.hazelcast.heartattack;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import java.io.*;
import java.net.URI;
import java.util.*;
import java.util.logging.Level;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import static java.lang.String.format;

public final class Utils {
    private final static ILogger log = Logger.getLogger(Utils.class);

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
            InputStream is = Utils.class.getResourceAsStream("/META-INF/maven/hazelcast-heartattack/hazelcast-heartattack/pom.properties");
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

                    log.log(Level.FINE, "Zipping: " + file.getAbsolutePath());

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

    public static void unzip(byte[] content, final File destinationDir) throws IOException {
        byte[] buffer = new byte[1024];

        ZipInputStream zis = new ZipInputStream(new ByteArrayInputStream(content));
        ZipEntry zipEntry = zis.getNextEntry();

        while (zipEntry != null) {

            String fileName = zipEntry.getName();
            File file = new File(destinationDir + File.separator + fileName);

            log.log(Level.FINE, "Unzipping: " + file.getAbsolutePath());


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


    public static File getHeartAttackHome() {
        String heartAttackHome = System.getenv("HEART_ATTACK_HOME");
        if (heartAttackHome == null) {
            return new File(System.getProperty("user.dir"));
        } else {
            return new File(heartAttackHome);
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
}
