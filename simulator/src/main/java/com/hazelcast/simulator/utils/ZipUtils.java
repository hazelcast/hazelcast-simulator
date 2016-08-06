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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.util.Deque;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import static com.hazelcast.simulator.utils.CommonUtils.closeQuietly;
import static com.hazelcast.simulator.utils.FileUtils.copy;
import static com.hazelcast.simulator.utils.FileUtils.ensureExistingDirectory;

public final class ZipUtils {

    private static final int BUFFER_SIZE = 1024;

    private ZipUtils() {
    }

    public static byte[] zip(List<File> roots) throws IOException {
        Deque<File> queue = new LinkedList<File>();
        Set<String> names = new HashSet<String>();

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        ZipOutputStream zipOutputStream = new ZipOutputStream(outputStream);
        try {
            for (File root : roots) {
                URI base = root.isDirectory() ? root.toURI() : root.getParentFile().toURI();
                queue.push(root);
                while (!queue.isEmpty()) {
                    File file = queue.pop();
                    if (file.getName().equals(".DS_Store")) {
                        continue;
                    }

                    if (file.isDirectory()) {
                        addDirectory(zipOutputStream, queue, names, base, file);
                    } else {
                        addFile(zipOutputStream, base, file);
                    }
                }
            }
        } finally {
            closeQuietly(zipOutputStream);
        }
        return outputStream.toByteArray();
    }

    public static void unzip(byte[] content, final File destinationDir) throws IOException {
        byte[] buffer = new byte[BUFFER_SIZE];

        ZipInputStream zis = new ZipInputStream(new ByteArrayInputStream(content));
        try {
            ZipEntry zipEntry = zis.getNextEntry();
            while (zipEntry != null) {
                String fileName = zipEntry.getName();
                File file = new File(destinationDir + File.separator + fileName);

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
        } finally {
            closeQuietly(zis);
        }
    }

    private static void addDirectory(ZipOutputStream zipOutputStream, Deque<File> queue, Set<String> names, URI base, File file)
            throws IOException {
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
    }

    private static void addFile(ZipOutputStream zipOutputStream, URI base, File file) throws IOException {
        String name = base.relativize(file.toURI()).getPath();
        zipOutputStream.putNextEntry(new ZipEntry(name));
        copy(file, zipOutputStream);
        zipOutputStream.closeEntry();
    }
}
