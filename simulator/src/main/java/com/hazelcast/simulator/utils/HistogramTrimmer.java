package com.hazelcast.simulator.utils;

import org.HdrHistogram.Histogram;
import org.HdrHistogram.HistogramLogReader;
import org.HdrHistogram.HistogramLogWriter;

import java.io.File;
import java.io.FileNotFoundException;

import static java.util.concurrent.TimeUnit.SECONDS;

public class HistogramTrimmer {

    public static void main(String[] args) throws FileNotFoundException {
        File inputFile = new File(args[0]);
        File outputFile = new File(inputFile.getParent(), inputFile.getName() + ".tmp");
        Integer warmupSeconds = Integer.parseInt(args[1]);
        Integer coolDownSeconds = Integer.parseInt(args[2]);

        long collectFromMs = -1;
        HistogramLogReader reader = new HistogramLogReader(inputFile);
        HistogramLogWriter writer = new HistogramLogWriter(outputFile);
        for (; ; ) {
            Histogram histogram = (Histogram) reader.nextIntervalHistogram();
            if (histogram == null) {
                break;
            }

            if (collectFromMs == -1) {
                collectFromMs = SECONDS.toMillis(warmupSeconds) + histogram.getStartTimeStamp();
            }

            if (histogram.getStartTimeStamp() > collectFromMs) {
                Histogram out = new Histogram(
                        histogram.getLowestDiscernibleValue(),
                        histogram.getHighestTrackableValue(),
                        histogram.getNumberOfSignificantValueDigits());
                out.add(histogram);
                writer.outputIntervalHistogram(out);
            }
        }

        inputFile.renameTo(new File(args[0] + ".old"));
        outputFile.renameTo(new File(args[0]));
    }
}
