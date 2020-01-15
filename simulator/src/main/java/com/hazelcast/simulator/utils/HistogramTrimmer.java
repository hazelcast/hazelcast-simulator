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

/**
 * Trims data before the start time and after the end time. This is needed for the
 * warmup and cooldown functionality.
 */
public final class HistogramTrimmer {

    private HistogramTrimmer() {
    }

    public static void main(String[] args) throws FileNotFoundException {
        File inputFile = new File(args[0]);
        File outputFile = new File(inputFile.getParent(), inputFile.getName() + ".tmp");
        long startMillis = Long.parseLong(args[1]);
        long endMillis = Long.parseLong(args[2]);

        HistogramLogReader reader = new HistogramLogReader(inputFile);
        HistogramLogWriter writer = new HistogramLogWriter(outputFile);
        for (; ; ) {
            Histogram histogram = (Histogram) reader.nextIntervalHistogram();
            if (histogram == null) {
                break;
            }

            if (histogram.getStartTimeStamp() >= startMillis && histogram.getEndTimeStamp() <= endMillis) {
                Histogram out = new Histogram(
                        histogram.getLowestDiscernibleValue(),
                        histogram.getHighestTrackableValue(),
                        histogram.getNumberOfSignificantValueDigits());
                out.setStartTimeStamp(histogram.getStartTimeStamp());
                out.setEndTimeStamp(histogram.getEndTimeStamp());
                out.add(histogram);
                writer.outputIntervalHistogram(out);
            }
        }

        outputFile.renameTo(new File(args[0]));
    }
}
