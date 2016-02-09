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
import java.util.concurrent.TimeUnit;

public class AccuracyRadioButtons extends JPanel {

    private static final int DEFAULT_ACCURACY = (int) TimeUnit.MICROSECONDS.toMicros(10);
    private static final int[] MICRO_SECONDS_ACCURACY = {10, 50, 100, 500};
    private static final int[] MILLI_SECONDS_ACCURACY = {1, 10, 50, 100, 1000};

    private final Map<Integer, JRadioButton> radioButtonMap = new HashMap<Integer, JRadioButton>();
    private final List<Integer> accuracyList = new ArrayList<Integer>();
    private final ButtonGroup buttonGroup = new ButtonGroup();

    private Chart chart;

    public AccuracyRadioButtons() {
        setMaximumSize(new Dimension(Integer.MAX_VALUE, Integer.MAX_VALUE));
        setBorder(BorderFactory.createTitledBorder("Accuracy"));
        setLayout(new BoxLayout(this, BoxLayout.Y_AXIS));

        for (int accuracy : MICRO_SECONDS_ACCURACY) {
            addButton(TimeUnit.MICROSECONDS.toMicros(accuracy), accuracy + " µs");
        }
        for (int accuracy : MILLI_SECONDS_ACCURACY) {
            addButton(TimeUnit.MILLISECONDS.toMicros(accuracy), accuracy + " ms");
        }
    }

    private void addButton(long accuracy, String title) {
        addButton((int) accuracy, title, (accuracy == DEFAULT_ACCURACY));
    }

    private void addButton(int accuracy, String title, boolean selected) {
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
        radioButtonMap.put(accuracy, radioButton);
        accuracyList.add(accuracy);
        buttonGroup.add(radioButton);
    }

    private void update() {
        revalidate();
        if (chart != null) {
            chart.updateChart();
        }
    }

    private void selectNextRadioButton() {
        int accuracy = getEnabledAccuracy();
        int index = accuracyList.indexOf(accuracy);
        if (index < accuracyList.size() - 1) {
            accuracy = accuracyList.get(index + 1);
            radioButtonMap.get(accuracy).setSelected(true);
            update();
        }
    }

    private void selectPrevRadioButton() {
        int accuracy = getEnabledAccuracy();
        int index = accuracyList.indexOf(accuracy);
        if (index > 0) {
            accuracy = accuracyList.get(index - 1);
            radioButtonMap.get(accuracy).setSelected(true);
            update();
        }
    }

    public void setChart(Chart chart) {
        this.chart = chart;
    }

    public int getEnabledAccuracy() {
        for (Map.Entry<Integer, JRadioButton> entry : radioButtonMap.entrySet()) {
            JRadioButton radioButton = entry.getValue();
            if (radioButton.isSelected()) {
                return entry.getKey();
            }
        }
        return DEFAULT_ACCURACY;
    }
}
