package com.hazelcast.simulator.visualiser.ui;

import com.hazelcast.simulator.probes.probes.LinearHistogram;
import com.hazelcast.simulator.probes.probes.Result;
import com.hazelcast.simulator.probes.probes.impl.HdrLatencyProbeResult;
import com.hazelcast.simulator.probes.probes.impl.LatencyDistributionResult;
import com.hazelcast.simulator.visualiser.data.BenchmarkResults;
import com.hazelcast.simulator.visualiser.data.Model;
import com.hazelcast.simulator.visualiser.data.AggregatedDataSet;
import com.hazelcast.simulator.visualiser.data.UnsafeSimpleHistogramDataSet;
import org.HdrHistogram.AbstractHistogram;
import org.HdrHistogram.Histogram;
import org.HdrHistogram.HistogramIterationValue;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.chart.plot.XYPlot;
import org.jfree.data.statistics.SimpleHistogramBin;
import org.jfree.data.statistics.SimpleHistogramDataset;

import javax.swing.*;
import javax.swing.event.ChangeEvent;
import javax.swing.event.ChangeListener;
import java.awt.*;
import java.util.*;
import java.util.List;

public class Chart extends JPanel {

    private static final float ALPHA = 0.65f;

    private final JSlider mainHorizontalSlider;
    private final JSlider fineHorizontalSlider;
    private final JSlider verticalSlider;
    private final XYPlot plot;

    private final Model model;
    private final ProbesCheckboxes probesCheckboxes;

    public Chart(Model model, ProbesCheckboxes probesCheckboxes) {
        this.model = model;
        this.probesCheckboxes = probesCheckboxes;

        setLayout(new BorderLayout());

        mainHorizontalSlider = new JSlider();
        fineHorizontalSlider = new JSlider();
        verticalSlider = new JSlider();

        setMaximumSize(new Dimension(Integer.MAX_VALUE, Integer.MAX_VALUE));
        verticalSlider.setOrientation(JSlider.VERTICAL);
        ChartPanel chartPanel = new ChartPanel(null);
        add(chartPanel, BorderLayout.CENTER);

        JPanel horizontalSlidersPanel = new JPanel();
        horizontalSlidersPanel.setLayout(new BoxLayout(horizontalSlidersPanel, BoxLayout.Y_AXIS));
        horizontalSlidersPanel.add(mainHorizontalSlider);
        horizontalSlidersPanel.add(fineHorizontalSlider);

        add(horizontalSlidersPanel, BorderLayout.SOUTH);
        add(verticalSlider, BorderLayout.EAST);

        JFreeChart chart = ChartFactory.createHistogram("Latency Distribution", "Latency (µs)", "Operations", null,
                PlotOrientation.VERTICAL, true, true, true);
        plot = chart.getXYPlot();
        plot.setForegroundAlpha(ALPHA);
        chartPanel.setChart(chart);

        mainHorizontalSlider.addChangeListener(new ChangeListener() {
            @Override
            public void stateChanged(ChangeEvent e) {
                int value = mainHorizontalSlider.getValue();
                plot.getDomainAxis().setUpperBound(value);
                fineHorizontalSlider.setMaximum(value);
                fineHorizontalSlider.setValue(value);
            }
        });

        fineHorizontalSlider.addChangeListener(new ChangeListener() {
            @Override
            public void stateChanged(ChangeEvent e) {
                int value = fineHorizontalSlider.getValue();
                plot.getDomainAxis().setUpperBound(value);
            }
        });

        verticalSlider.addChangeListener(new ChangeListener() {
            @Override
            public void stateChanged(ChangeEvent e) {
                plot.getRangeAxis().setUpperBound(verticalSlider.getValue());
            }
        });
    }

    public void updateChart() {
        List<String> selectedProbes = probesCheckboxes.getEnabledProbes();
        AggregatedDataSet dataSet = calculateDataSet(selectedProbes);
        updateDataSet(dataSet);
    }

    private AggregatedDataSet calculateDataSet(List<String> selectedProbes) {
        AggregatedDataSet dataSet = new AggregatedDataSet();
        Set<String> benchmarkNames = model.getBenchmarkNames();
        for (String benchmarkName : benchmarkNames) {
            BenchmarkResults benchmarkResults = model.getBenchmarkResults(benchmarkName);
            for (String selectedProbe : selectedProbes) {
                Result probeData = benchmarkResults.getProbeData(selectedProbe);
                if (probeData instanceof LatencyDistributionResult) {
                    String name = benchmarkName + " - " + selectedProbe;
                    SimpleHistogramDataset simpleHistogramDataset = calculateSingleProbeDataset((LatencyDistributionResult) probeData);
                    dataSet.addNewSeries(simpleHistogramDataset, name);
                } else if (probeData instanceof HdrLatencyProbeResult) {
                    String name = benchmarkName + " - " + selectedProbe;
                    SimpleHistogramDataset simpleHistogramDataset = calculateSingleProbeDataset((HdrLatencyProbeResult) probeData);
                    dataSet.addNewSeries(simpleHistogramDataset, name);
                }
            }
        }
        return dataSet;
    }

    private SimpleHistogramDataset calculateSingleProbeDataset(HdrLatencyProbeResult probeData) {
        UnsafeSimpleHistogramDataSet simpleHistogramDataset = new UnsafeSimpleHistogramDataSet("key");
        simpleHistogramDataset.setAdjustForBinSize(false);
        Histogram histogram = probeData.getHistogram();
        AbstractHistogram.LinearBucketValues histogramIterationValues = histogram.linearBucketValues(10);
        for (HistogramIterationValue v : histogramIterationValues) {
            double lowerBound = v.getDoubleValueIteratedFrom();
            double upperBound = v.getDoubleValueIteratedTo();
            SimpleHistogramBin bin = new SimpleHistogramBin(lowerBound, upperBound, true, false);
            bin.setItemCount((int) v.getCountAddedInThisIterationStep());
            simpleHistogramDataset.addBin(bin);
        }
        return simpleHistogramDataset;
    }

    private SimpleHistogramDataset calculateSingleProbeDataset(LatencyDistributionResult probeData) {
        UnsafeSimpleHistogramDataSet simpleHistogramDataset = new UnsafeSimpleHistogramDataSet("key");
        LinearHistogram histogram = probeData.getHistogram();
        int[] buckets = histogram.getBuckets();
        int step = histogram.getStep();
        int binNo = 0;
        for (int values : buckets) {
            double lowerBound = step * binNo;
            binNo++;
            double upperBound = step * binNo;
            SimpleHistogramBin bin = new SimpleHistogramBin(lowerBound, upperBound, true, false);
            bin.setItemCount(values);
            simpleHistogramDataset.addBin(bin);
        }
        return simpleHistogramDataset;
    }

    private void updateDataSet(AggregatedDataSet dataSet) {
        plot.getDomainAxis().setAutoRange(true);
        plot.getRangeAxis().setAutoRange(true);
        plot.setDataset(dataSet);
        Double horizontalUpperBound = plot.getDomainAxis().getRange().getUpperBound();
        mainHorizontalSlider.setMaximum(horizontalUpperBound.intValue());
        mainHorizontalSlider.setValue(horizontalUpperBound.intValue());

        Double verticalUpperBound = plot.getRangeAxis().getRange().getUpperBound();
        verticalSlider.setMaximum(verticalUpperBound.intValue());
        verticalSlider.setValue(verticalUpperBound.intValue());
    }
}
