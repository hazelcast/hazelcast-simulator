package com.hazelcast.simulator.visualiser.io;

import com.hazelcast.simulator.probes.probes.Result;
import com.hazelcast.simulator.probes.probes.ProbesResultXmlReader;
import com.hazelcast.simulator.probes.probes.util.Utils;
import com.hazelcast.simulator.visualiser.data.BenchmarkResults;
import com.hazelcast.simulator.visualiser.data.Model;

import javax.swing.*;
import java.io.File;
import java.io.FileInputStream;
import java.util.Map;
import java.util.concurrent.ExecutionException;

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
            Utils.closeQuietly(fileInputStream);
        }
    }

    private static String getFileName(File file) {
        return removeExtension(file.getName());
    }

    private static String removeExtension(String name) {
        int dotPos = name.lastIndexOf('.');
        if (dotPos != -1) {
            name = name.substring(0, dotPos);
        }
        return name;
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
