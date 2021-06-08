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

package com.hazelcast.simulator.tests.platform.nexmark;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.internal.util.HashUtil;
import com.hazelcast.jet.JetService;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.accumulator.LongLongAccumulator;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.StreamStage;
import com.hazelcast.simulator.hz.HazelcastTest;
import com.hazelcast.simulator.tests.platform.nexmark.accumulator.PickAnyAccumulator;
import com.hazelcast.simulator.tests.platform.nexmark.model.Auction;
import com.hazelcast.simulator.tests.platform.nexmark.model.Bid;
import com.hazelcast.simulator.tests.platform.nexmark.model.Person;
import org.HdrHistogram.Histogram;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.io.Serializable;

import static com.hazelcast.jet.datamodel.Tuple2.tuple2;
import static com.hazelcast.simulator.tests.platform.nexmark.processor.EventSourceP.simpleTime;
import static java.util.concurrent.TimeUnit.SECONDS;

public abstract class BenchmarkBase extends HazelcastTest {
    static final long WARMUP_REPORTING_INTERVAL_MS = SECONDS.toMillis(2);
    static final long MEASUREMENT_REPORTING_INTERVAL_MS = SECONDS.toMillis(10);
    static final long BENCHMARKING_DONE_REPORT_INTERVAL_MS = SECONDS.toMillis(1);
    static final String BENCHMARK_DONE_MESSAGE = "benchmarking is done";
    static final long INITIAL_SOURCE_DELAY_MILLIS = 10;

    private int latencyReportingThresholdMs;


    BenchmarkBase() {
    }

    @SuppressWarnings("checkstyle:methodlength")
    public Job run(HazelcastInstance instance, BenchmarkProperties props) {
        JetService jet = instance.getJet();
        String benchmarkName = getClass().getSimpleName();
        JobConfig jobCfg = new JobConfig();
        jobCfg.setName(benchmarkName);
        jobCfg.registerSerializer(Auction.class, Auction.AuctionSerializer.class);
        jobCfg.registerSerializer(Bid.class, Bid.BidSerializer.class);
        jobCfg.registerSerializer(Person.class, Person.PersonSerializer.class);
        jobCfg.registerSerializer(PickAnyAccumulator.class, PickAnyAccumulator.PickAnyAccumulatorSerializer.class);
        try {
            logger.info(String.format("Benchmark name               %s%n", benchmarkName));
            logger.info(props.toString());
            this.latencyReportingThresholdMs = props.latencyReportingThresholdMs;
            long warmupTimeMillis = SECONDS.toMillis(props.warmupSeconds);
            long totalTimeMillis = SECONDS.toMillis(props.warmupSeconds + props.measurementSeconds);

            Pipeline pipeline = Pipeline.create();
            StreamStage<Tuple2<Long, Long>> latencies = addComputation(pipeline);
            latencies.filter(t2 -> t2.f0() < totalTimeMillis)
                    .map(t2 -> String.format("%d,%d", t2.f0(), t2.f1()))
                    .writeTo(Sinks.files(new File("nexmark", "log").getPath()));
            latencies
                    .mapStateful(
                            () -> new RecordLatencyHistogram(warmupTimeMillis, totalTimeMillis),
                            RecordLatencyHistogram::map)
                    .writeTo(Sinks.files(new File("nexmark", "histogram").getPath()));

            jobCfg.setProcessingGuarantee(props.guarantee);
            jobCfg.setSnapshotIntervalMillis(props.snapshotIntervalMillis);
            return jet.newJobIfAbsent(pipeline, jobCfg);
        } catch (ValidationException e) {
            System.err.println(e.getMessage());
        }
        return null;
    }

    abstract StreamStage<Tuple2<Long, Long>> addComputation(Pipeline pipeline) throws ValidationException;

    static long getRandom(long seq, long range) {
        return Math.abs(HashUtil.fastLongMix(seq)) % range;
    }

    <T> FunctionEx<StreamStage<T>, StreamStage<Tuple2<Long, Long>>> determineLatency(
            FunctionEx<? super T, ? extends Long> timestampFn
    ) {
        int latencyReportingThresholdLocal = this.latencyReportingThresholdMs;
        return stage ->
                stage.map(timestampFn)
                        .mapStateful(LongLongAccumulator::new, /* (startTimestamp, lastTimestamp)*/
                                (state, timestamp) -> {
                                    long lastTimestamp = state.get2();
                                    if (timestamp <= lastTimestamp) {
                                        return null;
                                    }
                                    if (lastTimestamp == 0) {
                                        state.set1(timestamp); // state.startTimestamp = timestamp;
                                    }
                                    long startTimestamp = state.get1();
                                    state.set2(timestamp); // state.lastTimestamp = timestamp;

                                    long latency = System.currentTimeMillis() - timestamp;
                                    if (latency == -1) { // very low latencies may be reported as negative due to clock skew
                                        latency = 0;
                                    }
                                    if (latency < 0) {
                                        throw new RuntimeException("Negative latency: " + latency);
                                    }
                                    long time = simpleTime(timestamp);
                                    if (latency >= (long) latencyReportingThresholdLocal) {
                                        System.out.format("time %,d: latency %,d ms%n", time, latency);
                                    }
                                    return tuple2(timestamp - startTimestamp, latency);
                                });
    }

    private static class RecordLatencyHistogram implements Serializable {
        private final long warmupTimeMillis;
        private final long totalTimeMillis;
        private long benchmarkDoneLastReport;
        private long warmingUpLastReport;
        private long measurementLastReport;
        private Histogram histogram = new Histogram(5);

        public RecordLatencyHistogram(long warmupTimeMillis, long totalTimeMillis) {
            this.warmupTimeMillis = warmupTimeMillis;
            this.totalTimeMillis = totalTimeMillis;
        }

        @SuppressWarnings("ConstantConditions")
        String map(Tuple2<Long, Long> timestampAndLatency) {
            long timestamp = timestampAndLatency.f0();
            String timeMsg = String.format("%,d ", totalTimeMillis - timestamp);
            if (histogram == null) {
                long benchmarkDoneNow = timestamp / BENCHMARKING_DONE_REPORT_INTERVAL_MS;
                if (benchmarkDoneNow > benchmarkDoneLastReport) {
                    benchmarkDoneLastReport = benchmarkDoneNow;
                    System.out.format(BENCHMARK_DONE_MESSAGE + " -- %s%n", timeMsg);
                }
                return null;
            }
            if (timestamp < warmupTimeMillis) {
                long warmingUpNow = timestamp / WARMUP_REPORTING_INTERVAL_MS;
                if (warmingUpNow > warmingUpLastReport) {
                    warmingUpLastReport = warmingUpNow;
                    System.out.format("warming up -- %s%n", timeMsg);
                }
            } else {
                long measurementNow = timestamp / MEASUREMENT_REPORTING_INTERVAL_MS;
                if (measurementNow > measurementLastReport) {
                    measurementLastReport = measurementNow;
                    System.out.println(timeMsg);
                }
                histogram.recordValue(timestampAndLatency.f1());
            }
            if (timestamp >= totalTimeMillis) {
                try {
                    return exportHistogram(histogram);
                } finally {
                    histogram = null;
                }
            }
            return null;
        }

        private static String exportHistogram(Histogram histogram) {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            PrintStream out = new PrintStream(bos);
            histogram.outputPercentileDistribution(out, 1.0);
            out.close();
            return bos.toString();
        }
    }
}
