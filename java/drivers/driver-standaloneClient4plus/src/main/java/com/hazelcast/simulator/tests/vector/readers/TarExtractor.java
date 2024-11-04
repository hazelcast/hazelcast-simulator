package com.hazelcast.simulator.tests.vector.readers;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;

public class TarExtractor {

    private static final Logger logger = LogManager.getLogger();

    private static final int BUFFER_SIZE = 100_000;

    public static void extractTarGZ(InputStream in, Path directory) throws IOException {
        GzipCompressorInputStream gzipIn = new GzipCompressorInputStream(in);
        try (TarArchiveInputStream tarIn = new TarArchiveInputStream(gzipIn)) {
            TarArchiveEntry entry;

            while ((entry = tarIn.getNextEntry()) != null) {
                // If the entry is a directory, create the directory.
                if (entry.isDirectory()) {
                    File f = new File(entry.getName());
                    boolean created = f.mkdir();
                    if (!created) {
                        logger.warn("Unable to create directory '{}', during extraction of archive contents.",
                                f.getAbsolutePath());
                    }
                } else {
                    int count;
                    byte[] data = new byte[BUFFER_SIZE];
                    var output = Path.of(directory.toString(), entry.getName());
                    FileOutputStream fos = new FileOutputStream(output.toFile(), false);
                    try (BufferedOutputStream dest = new BufferedOutputStream(fos, BUFFER_SIZE)) {
                        while ((count = tarIn.read(data, 0, BUFFER_SIZE)) != -1) {
                            dest.write(data, 0, count);
                        }
                    }
                }
            }

            logger.info("Untar completed successfully!");
        }
    }
}



