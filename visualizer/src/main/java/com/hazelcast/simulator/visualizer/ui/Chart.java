/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.simulator.visualizer.data.AggregatedDataSet;
import com.hazelcast.simulator.visualizer.data.Model;
import com.hazelcast.simulator.visualizer.data.SimulatorHistogramDataSet;
import org.HdrHistogram.Histogram;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.chart.plot.XYPlot;

import javax.swing.*;
import javax.swing.event.ChangeEvent;
import javax.swing.event.ChangeListener;
import java.awt.*;
import java.util.List;
import java.util.Set;

import static com.hazelcast.simulator.visualizer.utils.DataSetUtils.getHistogramDataSet;
import static com.hazelcast.simulator.visualizer.utils.ListenerUtils.addHorizontalMouseListener;
import static com.hazelcast.simulator.visualizer.utils.ListenerUtils.addVerticalMouseListener;

public class Chart extends JPanel {

    private static final int MAIN_SLIDER_MINIMUM = 100;
    private static final int FINE_SLIDER_MINIMUM = 50;
    private static final int VERTICAL_SLIDER_MINIMUM = 100;

    private static final float ALPHA = 0.65f;

    private final JSlider mainHorizontalSlider = new JSlider();
    private final JSlider fineHorizontalSlider = new JSlider();
    private final JSlider verticalSlider = new JSlider();

    private final Model model;
    private final AccuracyRadioButtons accuracyRadioButtons;
    private final AutoScaleRadioButtons autoScaleRadioButtons;
    private final ProbesCheckboxes probesCheckboxes;
    private final XYPlot plot;

    public Chart(Model model, AccuracyRadioButtons accuracyRadioButtons, AutoScaleRadioButtons autoScaleRadioButtons,
                 ProbesCheckboxes probesCheckboxes) {
        this.model = model;
        this.accuracyRadioButtons = accuracyRadioButtons;
        this.autoScaleRadioButtons = autoScaleRadioButtons;
        this.probesCheckboxes = probesCheckboxes;

        setMaximumSize(new Dimension(Integer.MAX_VALUE, Integer.MAX_VALUE));
        setLayout(new BorderLayout());

        initSliders();

        JPanel horizontalSlidersPanel = new JPanel();
        horizontalSlidersPanel.setLayout(new BoxLayout(horizontalSlidersPanel, BoxLayout.Y_AXIS));
        horizontalSlidersPanel.add(mainHorizontalSlider);
        horizontalSlidersPanel.add(fineHorizontalSlider);

        JFreeChart chart = ChartFactory.createHistogram("Latency Distribution", "Latency (µs)", "Operations", null,
                PlotOrientation.VERTICAL, true, true, true);

        plot = chart.getXYPlot();
        plot.setForegroundAlpha(ALPHA);

        ChartPanel chartPanel = new ChartPanel(null);
        chartPanel.setChart(chart);

        add(horizontalSlidersPanel, BorderLayout.SOUTH);
        add(verticalSlider, BorderLayout.EAST);
        add(chartPanel, BorderLayout.CENTER);

        initSliderChangeListener();
    }

    private void initSliders() {
        mainHorizontalSlider.setMinimum(MAIN_SLIDER_MINIMUM);
        fineHorizontalSlider.setMinimum(FINE_SLIDER_MINIMUM);
        verticalSlider.setMinimum(VERTICAL_SLIDER_MINIMUM);
        verticalSlider.setOrientation(JSlider.VERTICAL);
    }

    private void initSliderChangeListener() {
        mainHorizontalSlider.addChangeListener(new ChangeListener() {
            @Override
            public void stateChanged(ChangeEvent e) {
                int value = mainHorizontalSlider.getValue();
                plot.getDomainAxis().setUpperBound(value);
                plot.getDomainAxis().setLowerBound(0);
                fineHorizontalSlider.setMaximum(value);
                fineHorizontalSlider.setValue(value);
            }
        });

        fineHorizontalSlider.addChangeListener(new ChangeListener() {
            @Override
            public void stateChanged(ChangeEvent e) {
                int value = fineHorizontalSlider.getValue();
                plot.getDomainAxis().setUpperBound(value);
                plot.getDomainAxis().setLowerBound(0);
            }
        });

        verticalSlider.addChangeListener(new ChangeListener() {
            @Override
            public void stateChanged(ChangeEvent e) {
                plot.getRangeAxis().setUpperBound(verticalSlider.getValue());
                plot.getRangeAxis().setLowerBound(0);
            }
        });

        addHorizontalMouseListener(this, mainHorizontalSlider);
        addHorizontalMouseListener(this, fineHorizontalSlider);
        addVerticalMouseListener(this, verticalSlider);
    }

    public void updateChart() {
        List<String> selectedProbes = probesCheckboxes.getEnabledProbes();
        AggregatedDataSet dataSet = calculateDataSet(selectedProbes);
        updateDataSet(dataSet);
    }

    private AggregatedDataSet calculateDataSet(List<String> selectedProbes) {
        int accuracy = accuracyRadioButtons.getEnabledAccuracy();
        double scalingPercentile = autoScaleRadioButtons.getEnabledPercentile();
        AggregatedDataSet aggregatedDataSet = new AggregatedDataSet();
        Set<String> benchmarkNames = model.getBenchmarkNames();
        for (String benchmarkName : benchmarkNames) {
            Result result = model.getBenchmarkResult(benchmarkName);
            for (String selectedProbe : selectedProbes) {
                Histogram histogram = result.getHistogram(selectedProbe);
                if (histogram == null) {
                    continue;
                }
                String name = benchmarkName + " - " + selectedProbe;
                SimulatorHistogramDataSet dataSet = getHistogramDataSet(histogram, accuracy, scalingPercentile);
                if (dataSet != null) {
                    aggregatedDataSet.addNewSeries(dataSet, name);
                }
            }
        }
        return aggregatedDataSet;
    }

    private void updateDataSet(AggregatedDataSet dataSet) {
        plot.getDomainAxis().setAutoRange(true);
        plot.getRangeAxis().setAutoRange(true);
        plot.setDataset(dataSet);

        Double horizontalUpperBound = plot.getDomainAxis().getRange().getUpperBound();
        mainHorizontalSlider.setMaximum(horizontalUpperBound.intValue());
        mainHorizontalSlider.setValue(0);
        mainHorizontalSlider.setValue((int) dataSet.getAutoScaleValue());

        Double verticalUpperBound = plot.getRangeAxis().getRange().getUpperBound();
        verticalSlider.setMaximum(verticalUpperBound.intValue());
        verticalSlider.setValue(0);
        verticalSlider.setValue(verticalUpperBound.intValue());
    }
}
