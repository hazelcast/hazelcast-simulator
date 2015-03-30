package com.hazelcast.simulator.visualiser.ui;

import com.hazelcast.simulator.probes.probes.Result;
import com.hazelcast.simulator.visualiser.data.AggregatedDataSet;
import com.hazelcast.simulator.visualiser.data.BenchmarkResults;
import com.hazelcast.simulator.visualiser.data.Model;
import com.hazelcast.simulator.visualiser.data.SimpleHistogramDataSetContainer;
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

import static com.hazelcast.simulator.visualiser.utils.DataSetUtils.calculateSingleProbeDataSet;
import static com.hazelcast.simulator.visualiser.utils.ListenerUtils.addHorizontalMouseListener;
import static com.hazelcast.simulator.visualiser.utils.ListenerUtils.addVerticalMouseListener;

public class Chart extends JPanel {

    private static final int MAIN_SLIDER_MINIMUM = 5000;
    private static final int FINE_SLIDER_MINIMUM = 100;
    private static final int VERTICAL_SLIDER_MINIMUM = 100;

    private static final float ALPHA = 0.65f;

    private final JSlider mainHorizontalSlider = new JSlider();
    private final JSlider fineHorizontalSlider = new JSlider();
    private final JSlider verticalSlider = new JSlider();

    private final Model model;
    private final ProbesCheckboxes probesCheckboxes;
    private final XYPlot plot;

    public Chart(Model model, ProbesCheckboxes probesCheckboxes) {
        this.model = model;
        this.probesCheckboxes = probesCheckboxes;

        setMaximumSize(new Dimension(Integer.MAX_VALUE, Integer.MAX_VALUE));
        setLayout(new BorderLayout());

        mainHorizontalSlider.setMinimum(MAIN_SLIDER_MINIMUM);
        fineHorizontalSlider.setMinimum(FINE_SLIDER_MINIMUM);
        verticalSlider.setMinimum(VERTICAL_SLIDER_MINIMUM);
        verticalSlider.setOrientation(JSlider.VERTICAL);

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
        AggregatedDataSet aggregatedDataSet = new AggregatedDataSet();
        Set<String> benchmarkNames = model.getBenchmarkNames();
        for (String benchmarkName : benchmarkNames) {
            BenchmarkResults benchmarkResults = model.getBenchmarkResults(benchmarkName);
            for (String selectedProbe : selectedProbes) {
                Result probeData = benchmarkResults.getProbeData(selectedProbe);
                if (probeData == null) {
                    System.out.println("Couldn't find probe " + selectedProbe + " in benchmark " + benchmarkName);
                    continue;
                }
                String name = benchmarkName + " - " + selectedProbe;
                SimpleHistogramDataSetContainer dataSet = calculateSingleProbeDataSet(probeData);
                aggregatedDataSet.addNewSeries(dataSet, name);
            }
        }
        return aggregatedDataSet;
    }

    private void updateDataSet(AggregatedDataSet dataSet) {
        plot.getDomainAxis().setAutoRange(true);
        plot.getRangeAxis().setAutoRange(true);
        plot.setDataset(dataSet);

        Double horizontalUpperBound = Math.min(plot.getDomainAxis().getRange().getUpperBound(), dataSet.getMaxLatency());
        mainHorizontalSlider.setMaximum(horizontalUpperBound.intValue());
        mainHorizontalSlider.setValue(horizontalUpperBound.intValue());

        Double verticalUpperBound = plot.getRangeAxis().getRange().getUpperBound();
        verticalSlider.setMaximum(verticalUpperBound.intValue());
        verticalSlider.setValue(verticalUpperBound.intValue());
    }
}
