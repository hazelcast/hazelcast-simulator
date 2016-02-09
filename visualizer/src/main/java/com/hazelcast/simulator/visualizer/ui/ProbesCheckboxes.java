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
package com.hazelcast.simulator.visualizer.ui;

import com.hazelcast.simulator.probes.Result;
import com.hazelcast.simulator.visualizer.data.Model;

import javax.swing.*;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ProbesCheckboxes extends JPanel implements Model.BenchmarkChangeListener {

    private static final boolean AUTO_SELECT_PROBES = true;

    private final Map<String, JCheckBox> checkBoxMap = new HashMap<String, JCheckBox>();

    private final Model model;

    private Chart chart;

    public ProbesCheckboxes(Model model) {
        this.model = model;

        setMaximumSize(new Dimension(Integer.MAX_VALUE, Integer.MAX_VALUE));
        setBorder(BorderFactory.createTitledBorder("Probes"));
        setLayout(new BoxLayout(this, BoxLayout.Y_AXIS));
    }

    public void setChart(Chart chart) {
        this.chart = chart;
    }

    public List<String> getEnabledProbes() {
        List<String> results = new ArrayList<String>();
        for (Map.Entry<String, JCheckBox> entry : checkBoxMap.entrySet()) {
            JCheckBox checkBox = entry.getValue();
            if (checkBox.isSelected()) {
                results.add(entry.getKey());
            }
        }
        return results;
    }

    @Override
    public void benchmarkChanged(String benchmarkName) {
        Result result = model.getBenchmarkResult(benchmarkName);
        if (result != null) {
            for (String probeName : result.probeNames()) {
                addProbeIfNotExist(probeName);
            }
        }
    }

    private void addProbeIfNotExist(String name) {
        if (!checkBoxMap.containsKey(name)) {
            JCheckBox button = new JCheckBox(name);
            button.setSelected(AUTO_SELECT_PROBES);
            button.addActionListener(new ActionListener() {
                @Override
                public void actionPerformed(ActionEvent e) {
                    update();
                }
            });
            add(button);
            checkBoxMap.put(name, button);
        }
        update();
    }

    private void update() {
        revalidate();
        if (chart != null) {
            chart.updateChart();
        }
    }
}
