package com.hazelcast.stabilizer.visualiser.io;

import com.hazelcast.stabilizer.probes.probes.Result;
import com.hazelcast.stabilizer.probes.probes.ProbesResultXmlReader;
import com.hazelcast.stabilizer.probes.probes.util.Utils;
import com.hazelcast.stabilizer.visualiser.data.BenchmarkResults;
import com.hazelcast.stabilizer.visualiser.data.Model;

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
        ProbesResultXmlReader reader = new ProbesResultXmlReader();
        FileInputStream is = null;
        try {
            is = new FileInputStream(file);
            Map<String, Result> read = reader.read(is);
            String filename = getName();
            BenchmarkResults results = new BenchmarkResults(filename);
            for (Map.Entry<String, Result> entry : read.entrySet()) {
                results.addProbeData(entry.getKey(), entry.getValue());
            }
            return results;
        } finally {
            Utils.closeQuietly(is);
        }
    }

    private String getName() {
        String filename = removeExtension(file.getName());
        return filename;
    }

    private String removeExtension(String name) {
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
