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

import org.HdrHistogram.Histogram;
import org.HdrHistogram.HistogramLogReader;
import org.HdrHistogram.HistogramLogWriter;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.simulator.utils.FileUtils.deleteQuiet;
import static com.hazelcast.simulator.utils.FileUtils.ensureExistingFile;

/**
 * The HistogramLogMerger merges the Histograms of multiple HDR files. This will be done based under the assumption that
 * every histogram spans the same duration.
 * <p>
 * It works like this; from each HistogramLogReader a Histogram is read and merged into a final Histogram and this Histogram
 * is written into the merged HDR. Once round is complete, the next Histogram is retrieved. The different HDR files don't need
 * to have the same length; as soon as a HDR is finished, it is ignored and this continues till all HDR files are fully processed.
 */
public final class HistogramLogMerger {

    private HistogramLogMerger() {
    }

    public static void main(String[] args) throws IOException {
        File outputFile = new File(args[0]);
        deleteQuiet(outputFile);
        ensureExistingFile(outputFile);

        if (args.length < 2) {
            throw new IllegalArgumentException("Usage: HistogramLogMerger <outputFile> <hdr_files_list_file>");
        }

        File inputFilesListFile = new File(args[1]);
        if (!inputFilesListFile.exists()) {
            throw new IllegalArgumentException("hdr_files_list_file [" + inputFilesListFile + "] doesn't exist");
        }

        log("Using input files list from " + inputFilesListFile);

        List<String> inputFiles = Files.readAllLines(inputFilesListFile.toPath());
        ArrayList<HistogramLogReader> readers = new ArrayList<>(inputFiles.size());
        inputFiles.forEach(p -> {
            File file = new File(p);
            if (!file.exists()) {
                throw new IllegalArgumentException("File [" + file + "] doesn't exist");
            }
            try {
                readers.add(new HistogramLogReader(p));
            } catch (FileNotFoundException e) {
                throw new RuntimeException(e);
            }
        });

        HistogramLogWriter writer = new HistogramLogWriter(outputFile);
        String comment = "[Latency histograms for " + getBaseName(outputFile) + ']';
        log(comment);
        writer.outputComment(comment);
        writer.outputLogFormatVersion();
        writer.outputLegend();

        int numberOfMergedHistograms = 0;
        for (; ; ) {
            Histogram merged = null;
            for (HistogramLogReader reader : readers) {
                Histogram histogram = (Histogram) reader.nextIntervalHistogram();
                if (histogram == null) {
                    continue;
                }

                if (merged == null) {
                    merged = new Histogram(
                            histogram.getLowestDiscernibleValue(),
                            histogram.getHighestTrackableValue(),
                            histogram.getNumberOfSignificantValueDigits());
                }
                merged.add(histogram);
            }

            if (merged == null) {
                break;
            }

            writer.outputIntervalHistogram(merged);
            log("Added merged histogram " + ++numberOfMergedHistograms + " to " + outputFile);
        }
    }

    private static void log(String log) {
        System.out.println("[HistogramLogMerger] " + log);
    }

    private static String getBaseName(File file) {
        String name = file.getName();
        int pos = name.lastIndexOf('.');
        return pos == -1 ? name : name.substring(0, pos);
    }
}
