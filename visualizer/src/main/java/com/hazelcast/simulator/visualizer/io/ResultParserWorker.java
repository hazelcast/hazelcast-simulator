/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast.simulator.visualizer.io;

import com.hazelcast.simulator.probes.probes.ProbesResultXmlReader;
import com.hazelcast.simulator.probes.probes.Result;
import com.hazelcast.simulator.visualizer.data.BenchmarkResults;
import com.hazelcast.simulator.visualizer.data.Model;

import javax.swing.*;
import java.io.File;
import java.io.FileInputStream;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static com.hazelcast.simulator.utils.CommonUtils.closeQuietly;
import static com.hazelcast.simulator.utils.FileUtils.getFileName;

public class ResultParserWorker extends SwingWorker<BenchmarkResults, Void> {

    private final File file;
    private final Model model;

    public ResultParserWorker(File file, Model model) {
        this.file = file;
        this.model = model;
    }

    @Override
    protected BenchmarkResults doInBackground() throws Exception {
        FileInputStream fileInputStream = null;
        try {
            fileInputStream = new FileInputStream(file);
            Map<String, Result> probeResultMap = ProbesResultXmlReader.read(fileInputStream);
            String filename = getFileName(file);

            BenchmarkResults benchmarkResults = new BenchmarkResults(filename);
            for (Map.Entry<String, Result> entry : probeResultMap.entrySet()) {
                benchmarkResults.addProbeData(entry.getKey(), entry.getValue());
            }
            return benchmarkResults;
        } finally {
            closeQuietly(fileInputStream);
        }
    }

    @Override
    protected void done() {
        try {
            BenchmarkResults results = get();
            model.addBenchmarkResults(results);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }
}
