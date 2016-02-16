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

import javax.swing.*;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.MouseWheelEvent;
import java.awt.event.MouseWheelListener;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AutoScaleRadioButtons extends JPanel {

    private static final double DEFAULT_SCALE = 0.99d;
    private static final double DOUBLE_PRECISION = 0.0001;
    private static final double[] SCALE_PERCENTILES = {1, 0.9999, 0.999, 0.99, 0.98, 0.97, 0.95, 0.9, 0.85, 0.8, 0.7, 0.5};

    private final Map<Double, JRadioButton> radioButtonMap = new HashMap<Double, JRadioButton>();
    private final List<Double> percentileList = new ArrayList<Double>();
    private final ButtonGroup buttonGroup = new ButtonGroup();

    private Chart chart;

    public AutoScaleRadioButtons() {
        setMaximumSize(new Dimension(Integer.MAX_VALUE, Integer.MAX_VALUE));
        setBorder(BorderFactory.createTitledBorder("AutoScale Percentile"));
        setLayout(new BoxLayout(this, BoxLayout.Y_AXIS));

        for (double percentile : SCALE_PERCENTILES) {
            addButton(percentile, String.valueOf(percentile));
        }
    }

    private void addButton(double percentile, String title) {
        addButton(percentile, title, (percentile - DEFAULT_SCALE < DOUBLE_PRECISION));
    }

    private void addButton(double percentile, String title, boolean selected) {
        JRadioButton radioButton = new JRadioButton(title);
        if (selected) {
            radioButton.setSelected(true);
        }
        radioButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                update();
            }
        });
        radioButton.addMouseWheelListener(new MouseWheelListener() {
            @Override
            public void mouseWheelMoved(MouseWheelEvent e) {
                if (e.getWheelRotation() > 0) {
                    selectNextRadioButton();
                } else {
                    selectPrevRadioButton();
                }
            }
        });
        add(radioButton);
        radioButtonMap.put(percentile, radioButton);
        percentileList.add(percentile);
        buttonGroup.add(radioButton);
    }

    private void update() {
        revalidate();
        if (chart != null) {
            chart.updateChart();
        }
    }

    private void selectNextRadioButton() {
        double percentile = getEnabledPercentile();
        int index = percentileList.indexOf(percentile);
        if (index < percentileList.size() - 1) {
            percentile = percentileList.get(index + 1);
            radioButtonMap.get(percentile).setSelected(true);
            update();
        }
    }

    private void selectPrevRadioButton() {
        double percentile = getEnabledPercentile();
        int index = percentileList.indexOf(percentile);
        if (index > 0) {
            percentile = percentileList.get(index - 1);
            radioButtonMap.get(percentile).setSelected(true);
            update();
        }
    }

    public void setChart(Chart chart) {
        this.chart = chart;
    }

    public double getEnabledPercentile() {
        for (Map.Entry<Double, JRadioButton> entry : radioButtonMap.entrySet()) {
            JRadioButton radioButton = entry.getValue();
            if (radioButton.isSelected()) {
                return entry.getKey();
            }
        }
        return DEFAULT_SCALE;
    }
}
