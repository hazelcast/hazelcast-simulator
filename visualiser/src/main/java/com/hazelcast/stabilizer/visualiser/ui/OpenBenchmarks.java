package com.hazelcast.stabilizer.visualiser.ui;

import com.hazelcast.stabilizer.visualiser.Model;

import javax.swing.*;
import java.awt.*;

public class OpenBenchmarks extends JPanel implements Model.BenchmarkChangeListener {
    private final Model model;

    public OpenBenchmarks(Model model) {
        setMaximumSize(new Dimension(Integer.MAX_VALUE, Integer.MAX_VALUE));
        BoxLayout layout = new BoxLayout(this, BoxLayout.Y_AXIS);
        setLayout(layout);
        setBorder(BorderFactory.createTitledBorder("Benchmarks"));
        this.model = model;
    }


    @Override
    public void benchmarkChanged(String benchmarkName) {
        JLabel label = new JLabel(benchmarkName);
        add(label);
    }
}
