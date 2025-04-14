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

import org.HdrHistogram.DoubleHistogram;
import org.HdrHistogram.Histogram;

import java.io.Closeable;
import java.io.FileNotFoundException;

@SuppressWarnings({"checkstyle:methodlength", "checkstyle:magicnumber"})
public class SimulatorHistogramLogProcessor extends HistogramLogProcessor implements Closeable {

    public SimulatorHistogramLogProcessor(String[] args) throws FileNotFoundException {
        super(args);
    }

    @Override
    protected Object[] buildRegularHistogramStatistics(Histogram intervalHistogram, Histogram accumulatedHistogram) {
        double intervalThroughput = ((double) (intervalHistogram.getTotalCount())
                / (intervalHistogram.getEndTimeStamp() - intervalHistogram.getStartTimeStamp()));

        double totalThroughput = ((double) accumulatedHistogram.getTotalCount())
                / (accumulatedHistogram.getEndTimeStamp() - accumulatedHistogram.getStartTimeStamp());

        return new Object[]{
                ((intervalHistogram.getEndTimeStamp() / 1000.0) - logReader.getStartTimeSec()),
                (intervalHistogram.getEndTimeStamp() / 1000.0),
                // values recorded during the last reporting interval
                intervalHistogram.getTotalCount(),
                intervalHistogram.getValueAtPercentile(25.0) / config.outputValueUnitRatio,
                intervalHistogram.getValueAtPercentile(50.0) / config.outputValueUnitRatio,
                intervalHistogram.getValueAtPercentile(75.0) / config.outputValueUnitRatio,
                intervalHistogram.getValueAtPercentile(90.0) / config.outputValueUnitRatio,
                intervalHistogram.getValueAtPercentile(99.0) / config.outputValueUnitRatio,
                intervalHistogram.getValueAtPercentile(99.9) / config.outputValueUnitRatio,
                intervalHistogram.getValueAtPercentile(99.99) / config.outputValueUnitRatio,
                intervalHistogram.getValueAtPercentile(99.999) / config.outputValueUnitRatio,
                intervalHistogram.getMinValue() / config.outputValueUnitRatio,
                intervalHistogram.getMaxValue() / config.outputValueUnitRatio,
                intervalHistogram.getMean() / config.outputValueUnitRatio,
                intervalHistogram.getStdDeviation() / config.outputValueUnitRatio,
                intervalThroughput / config.outputValueUnitRatio,

                // values recorded from the beginning until now
                accumulatedHistogram.getTotalCount(),
                accumulatedHistogram.getValueAtPercentile(25.0) / config.outputValueUnitRatio,
                accumulatedHistogram.getValueAtPercentile(50.0) / config.outputValueUnitRatio,
                accumulatedHistogram.getValueAtPercentile(75.0) / config.outputValueUnitRatio,
                accumulatedHistogram.getValueAtPercentile(90.0) / config.outputValueUnitRatio,
                accumulatedHistogram.getValueAtPercentile(99.0) / config.outputValueUnitRatio,
                accumulatedHistogram.getValueAtPercentile(99.9) / config.outputValueUnitRatio,
                accumulatedHistogram.getValueAtPercentile(99.99) / config.outputValueUnitRatio,
                accumulatedHistogram.getValueAtPercentile(99.999) / config.outputValueUnitRatio,
                accumulatedHistogram.getMinValue() / config.outputValueUnitRatio,
                accumulatedHistogram.getMaxValue() / config.outputValueUnitRatio,
                accumulatedHistogram.getMean() / config.outputValueUnitRatio,
                accumulatedHistogram.getStdDeviation() / config.outputValueUnitRatio,
                totalThroughput / config.outputValueUnitRatio,
        };
    }

    @Override
    protected Object[] buildDoubleHistogramStatistics(DoubleHistogram intervalHistogram, DoubleHistogram accumulatedHistogram) {
        double intervalThroughput = ((double) (intervalHistogram.getTotalCount())
                / (intervalHistogram.getEndTimeStamp() - intervalHistogram.getStartTimeStamp()));

        double totalThroughput = ((double) accumulatedHistogram.getTotalCount())
                / (accumulatedHistogram.getEndTimeStamp() - accumulatedHistogram.getStartTimeStamp());

        return new Object[]{
                ((intervalHistogram.getEndTimeStamp() / 1000.0) - logReader.getStartTimeSec()),
                (intervalHistogram.getEndTimeStamp() / 1000.0),
                // values recorded during the last reporting interval
                intervalHistogram.getTotalCount(),
                intervalHistogram.getValueAtPercentile(25.0) / config.outputValueUnitRatio,
                intervalHistogram.getValueAtPercentile(50.0) / config.outputValueUnitRatio,
                intervalHistogram.getValueAtPercentile(75) / config.outputValueUnitRatio,
                intervalHistogram.getValueAtPercentile(90.0) / config.outputValueUnitRatio,
                intervalHistogram.getValueAtPercentile(99.0) / config.outputValueUnitRatio,
                intervalHistogram.getValueAtPercentile(99.9) / config.outputValueUnitRatio,
                intervalHistogram.getValueAtPercentile(99.99) / config.outputValueUnitRatio,
                intervalHistogram.getValueAtPercentile(99.999) / config.outputValueUnitRatio,
                intervalHistogram.getMinValue() / config.outputValueUnitRatio,
                intervalHistogram.getMaxValue() / config.outputValueUnitRatio,
                intervalHistogram.getMean() / config.outputValueUnitRatio,
                intervalHistogram.getStdDeviation() / config.outputValueUnitRatio,
                intervalThroughput / config.outputValueUnitRatio,

                // values recorded from the beginning until now
                accumulatedHistogram.getTotalCount(),
                accumulatedHistogram.getValueAtPercentile(25.0) / config.outputValueUnitRatio,
                accumulatedHistogram.getValueAtPercentile(50.0) / config.outputValueUnitRatio,
                accumulatedHistogram.getValueAtPercentile(75.0) / config.outputValueUnitRatio,
                accumulatedHistogram.getValueAtPercentile(90.0) / config.outputValueUnitRatio,
                accumulatedHistogram.getValueAtPercentile(99.0) / config.outputValueUnitRatio,
                accumulatedHistogram.getValueAtPercentile(99.9) / config.outputValueUnitRatio,
                accumulatedHistogram.getValueAtPercentile(99.99) / config.outputValueUnitRatio,
                accumulatedHistogram.getValueAtPercentile(99.999) / config.outputValueUnitRatio,
                accumulatedHistogram.getMinValue() / config.outputValueUnitRatio,
                accumulatedHistogram.getMaxValue() / config.outputValueUnitRatio,
                accumulatedHistogram.getMean() / config.outputValueUnitRatio,
                accumulatedHistogram.getStdDeviation() / config.outputValueUnitRatio,
                totalThroughput / config.outputValueUnitRatio,
        };
    }

    @Override
    protected String buildLegend(boolean cvs) {
        if (cvs) {
            return "\"Timestamp\","
                    + "\"StartTime\","
                    + "\"Int_Count\","
                    + "\"Int_25%\","
                    + "\"Int_50%\","
                    + "\"Int_75%\","
                    + "\"Int_90%\","
                    + "\"Int_99%\","
                    + "\"Int_99.9%\","
                    + "\"Int_99.99%\","
                    + "\"Int_99.999%\","
                    + "\"Int_Min\","
                    + "\"Int_Max\","
                    + "\"Int_Mean\","
                    + "\"Int_Std_Deviation\","
                    + "\"Int_Throughput\","
                    + "\"Total_Count\","
                    + "\"Total_25%\","
                    + "\"Total_50%\","
                    + "\"Total_75%\","
                    + "\"Total_90%\","
                    + "\"Total_99%\","
                    + "\"Total_99.9%\","
                    + "\"Total_99.99%\","
                    + "\"Total_99.999%\","
                    + "\"Total_Min%\","
                    + "\"Total_Max\","
                    + "\"Total_Mean\","
                    + "\"Total_Std_Deviation\","
                    + "\"Total_Throughput\","
                    ;
        } else {
            return "Time: StartTime: IntervalPercentiles:count "
                    + "( 25% 50% 75% 90% 99.9% 99.99% 99.999% Min Max Mean Std-Deviation Throughput) "
                    + "TotalPercentiles:count "
                    + "( 25% 50% 75% 90% 99% 99.9% 99.99% 99.9% Min Max Mean Std-Deviation Throughput)";
        }
    }

    @Override
    protected String buildLogFormat(boolean cvs) {
        if (cvs) {
            return "%.3f," //timestamp
                    + "%.3f," //timestamp
                    + "%d," //int count
                    + "%.3f," //int 25%
                    + "%.3f," //int 50%
                    + "%.3f," //int 75%
                    + "%.3f," //int 90%
                    + "%.3f," //int 99%
                    + "%.3f," //int 99.9%
                    + "%.3f," //int 99.99%
                    + "%.3f," //int 99.999%
                    + "%.3f," //int min
                    + "%.3f," //int max
                    + "%.3f," //int mean
                    + "%.3f," //int std deviation
                    + "%.3f," //int throughput

                    + "%d," //total count
                    + "%.3f," //total 25%
                    + "%.3f," //total 50%
                    + "%.3f," //total 75%
                    + "%.3f," //total 90%
                    + "%.3f," //total 99%
                    + "%.3f," //total 99.9%
                    + "%.3f," //total 99.99%
                    + "%.3f," //total 99.999%
                    + "%.3f," //total-min
                    + "%.3f," //total-max
                    + "%.3f," //total-mean
                    + "%.3f," //total std deviation
                    + "%.3f" //total throughput
                    + "\n"
            ;
        } else {
            return "%4.3f: %4.3f: I"
                    + ":%d " //int count
                    + "( "
                    + "%7.3f " //int 25%
                    + "%7.3f " //int 50%
                    + "%7.3f " //int 75%
                    + "%7.3f " //int 90%
                    + "%7.3f " //int 99%
                    + "%7.3f " //int 99.9%
                    + "%7.3f " //int 99.99%
                    + "%7.3f " //int 99.999%
                    + "%7.3f " //int min
                    + "%7.3f " //int max
                    + "%7.3f " //int mean
                    + "%7.3f " //int std deviation
                    + "%7.3f " //int throughput
                    + "( "
                    + "T:%d " //total count
                    + "( "
                    + "%7.3f " //total 25%
                    + "%7.3f " //total 50%
                    + "%7.3f " //total 75%
                    + "%7.3f " //total 99%
                    + "%7.3f " //total 99.9%
                    + "%7.3f " //total 99.99%
                    + "%7.3f " //total 99.999%
                    + "%7.3f " //total min
                    + "%7.3f " //total max
                    + "%7.3f " //total mean
                    + "%7.3f " //total std deviation
                    + "%7.3f " //total throughput
                    + ")\n"
                    ;
        }
    }

    @Override
    public void close() {
        logReader.close();
    }

    public static void main(final String[] args) {
        try (SimulatorHistogramLogProcessor processor = new SimulatorHistogramLogProcessor(args)) {
            processor.run();
        } catch (FileNotFoundException ex) {
            System.err.println("Failed to open input file: " + ex.getMessage());
        }
    }
}
