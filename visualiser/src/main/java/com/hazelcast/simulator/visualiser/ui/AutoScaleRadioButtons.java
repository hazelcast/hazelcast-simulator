package com.hazelcast.simulator.visualiser.ui;

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

    private final Map<Double, JRadioButton> radioButtonMap = new HashMap<Double, JRadioButton>();
    private final List<Double> percentileList = new ArrayList<Double>();
    private final ButtonGroup buttonGroup = new ButtonGroup();

    private Chart chart;

    public AutoScaleRadioButtons() {
        setMaximumSize(new Dimension(Integer.MAX_VALUE, Integer.MAX_VALUE));
        setBorder(BorderFactory.createTitledBorder("AutoScale Percentile"));
        setLayout(new BoxLayout(this, BoxLayout.Y_AXIS));

        addButton(1.0d, "1.0");
        addButton(0.9999d, "0.9999");
        addButton(0.999d, "0.999");
        addButton(0.99d, "0.99");
        addButton(0.98d, "0.98");
        addButton(0.97d, "0.97");
        addButton(0.95d, "0.95");
        addButton(0.9d, "0.9");
        addButton(0.85d, "0.85");
        addButton(0.8d, "0.8");
        addButton(0.7d, "0.7");
        addButton(0.5d, "0.5");
    }

    private void addButton(double percentile, String title) {
        addButton(percentile, title, (percentile - DEFAULT_SCALE < 0.0001));
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
