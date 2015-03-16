package com.hazelcast.simulator.visualiser.ui;

import com.hazelcast.simulator.visualiser.data.BenchmarkResults;
import com.hazelcast.simulator.visualiser.data.Model;

import javax.swing.*;
import java.awt.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ProbesCheckboxes extends JPanel implements Model.BenchmarkChangeListener {

    private final Map<String, JCheckBox> checkBoxMap = new HashMap<String, JCheckBox>();
    private final Model model;

    public ProbesCheckboxes(Model model) {
        setMaximumSize(new Dimension(Integer.MAX_VALUE, Integer.MAX_VALUE));
        setBorder(BorderFactory.createTitledBorder("Probes"));
        setLayout(new BoxLayout(this, BoxLayout.Y_AXIS));

        this.model = model;
    }

    public List<String> getEnabledProbes() {
        List<String> results = new ArrayList<String>();
        for (Map.Entry<String, JCheckBox> entry: checkBoxMap.entrySet()) {
            JCheckBox checkBox = entry.getValue();
            if (checkBox.isSelected()) {
                results.add(entry.getKey());
            }
        }
        return results;
    }

    @Override
    public void benchmarkChanged(String benchmarkName) {
        BenchmarkResults benchmarkResults = model.getBenchmarkResults(benchmarkName);
        if (benchmarkResults != null) {
            Set<String> probeNames = benchmarkResults.getProbeNames();
            for (String probeName : probeNames) {
                addProbeIfNotExist(probeName);
            }
        }
    }

    private void addProbeIfNotExist(String name) {
        JCheckBox button = new JCheckBox(name);
        if (!checkBoxMap.containsKey(name)) {
            checkBoxMap.put(name, button);
            add(button);
        }
        revalidate();
    }
}
