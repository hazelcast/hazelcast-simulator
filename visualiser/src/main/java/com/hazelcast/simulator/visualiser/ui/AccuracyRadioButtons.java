package com.hazelcast.simulator.visualiser.ui;

import javax.swing.*;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class AccuracyRadioButtons extends JPanel {

    private static final int DEFAULT_ACCURACY = (int) TimeUnit.MICROSECONDS.toMicros(10);

    private final Map<Integer, JRadioButton> radioButtonMap = new HashMap<Integer, JRadioButton>();
    private final ButtonGroup buttonGroup = new ButtonGroup();

    private Chart chart;

    public AccuracyRadioButtons() {
        setMaximumSize(new Dimension(Integer.MAX_VALUE, Integer.MAX_VALUE));
        setBorder(BorderFactory.createTitledBorder("Accuracy"));
        setLayout(new BoxLayout(this, BoxLayout.Y_AXIS));

        addButton(TimeUnit.MICROSECONDS.toMicros(10), "10 µs");
        addButton(TimeUnit.MICROSECONDS.toMicros(50), "50 µs");
        addButton(TimeUnit.MICROSECONDS.toMicros(100), "100 µs");
        addButton(TimeUnit.MICROSECONDS.toMicros(500), "500 µs");
        addButton(TimeUnit.MILLISECONDS.toMicros(1), "1 ms");
        addButton(TimeUnit.MILLISECONDS.toMicros(10), "10 ms");
        addButton(TimeUnit.MILLISECONDS.toMicros(50), "50 ms");
        addButton(TimeUnit.MILLISECONDS.toMicros(100), "100 ms");
        addButton(TimeUnit.MILLISECONDS.toMicros(1000), "1000 ms");
    }

    private void addButton(long accuracy, String title) {
        addButton(accuracy, title, (accuracy == DEFAULT_ACCURACY));
    }

    private void addButton(long accuracy, String title, boolean selected) {
        JRadioButton radioButton = new JRadioButton(title);
        if (selected) {
            radioButton.setSelected(true);
        }
        radioButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                if (chart != null) {
                    chart.updateChart();
                }
            }
        });
        add(radioButton);
        radioButtonMap.put((int) accuracy, radioButton);
        buttonGroup.add(radioButton);
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

    public void setChart(Chart chart) {
        this.chart = chart;
    }
}
