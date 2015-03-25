package com.hazelcast.simulator.visualiser.ui;

import com.hazelcast.simulator.visualiser.data.Model;

import javax.swing.*;
import java.awt.*;

public class LoadedBenchmarks extends JPanel implements Model.BenchmarkChangeListener {

    public LoadedBenchmarks() {
        setMaximumSize(new Dimension(Integer.MAX_VALUE, Integer.MAX_VALUE));
        setBorder(BorderFactory.createTitledBorder("Benchmarks"));
        setLayout(new BoxLayout(this, BoxLayout.Y_AXIS));
    }

    @Override
    public void benchmarkChanged(String benchmarkName) {
        JLabel label = new JLabel(benchmarkName);
        add(label);
    }
}
