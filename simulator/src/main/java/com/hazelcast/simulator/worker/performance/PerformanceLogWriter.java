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
package com.hazelcast.simulator.worker.performance;

import java.io.File;
import java.text.DecimalFormat;

import static com.hazelcast.simulator.utils.FileUtils.appendText;
import static com.hazelcast.simulator.utils.Preconditions.checkNotNull;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Responsible for writing to performance stats to disk in csv format.
 */
final class PerformanceLogWriter {

    private final StringBuffer sb = new StringBuffer();
    private final DecimalFormat format = new DecimalFormat("#.###");
    private final File file;
    private boolean headerWritten;

    PerformanceLogWriter(File file) {
        this.file = checkNotNull(file, "file can't be null");
    }

    private void writeHeader() {
        String columns = "epoch,timestamp,operations,operations-delta,operations/second\n";
        appendText(columns, file);
    }

    void write(long timeMillis,
               String timestamp,
               long operationsTotal,
               long operationsDelta,
               double operationsPerSecond) {

        if (!headerWritten) {
            writeHeader();
            headerWritten = true;
        }

        sb.setLength(0);
        // ms are expressed in epoch time after the decimal point
        sb.append(format.format(timeMillis * 1d / SECONDS.toMillis(1)));
        sb.append(',').append(timestamp);
        sb.append(',').append(operationsTotal);
        sb.append(',').append(operationsDelta);
        sb.append(',').append(format.format(operationsPerSecond));
        sb.append('\n');
        appendText(sb.toString(), file);
    }
}
