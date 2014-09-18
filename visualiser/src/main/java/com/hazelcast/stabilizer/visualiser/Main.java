package com.hazelcast.stabilizer.visualiser;

import com.hazelcast.stabilizer.common.LinearHistogram;
import com.hazelcast.stabilizer.common.probes.Result;
import com.hazelcast.stabilizer.common.probes.impl.LatencyDistributionResult;
import com.hazelcast.stabilizer.visualiser.io.ResultParserWorker;

import javax.swing.*;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

import java.awt.event.KeyEvent;
import java.io.File;
import java.util.List;
import java.util.Set;

import javax.swing.JFrame;
import javax.swing.SwingUtilities;

import com.hazelcast.stabilizer.visualiser.ui.MyDataSet;
import com.hazelcast.stabilizer.visualiser.ui.OpenBenchmarks;
import com.hazelcast.stabilizer.visualiser.ui.ProbesCheckboxes;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.chart.plot.XYPlot;
import org.jfree.data.statistics.SimpleHistogramBin;
import org.jfree.data.statistics.SimpleHistogramDataset;
import org.jfree.ui.ExtensionFileFilter;

public class Main {

    private JFrame frame;
    private ChartPanel cp;
    private Model model;
    private XYPlot plot;

    public Main() {
        model = new Model();
        createUI();
        createMenu();
    }

    private void createMenu() {
        JMenuBar menuBar = new JMenuBar();
        JMenu menu = new JMenu("File");
        menu.setMnemonic(KeyEvent.VK_A);
        menuBar.add(menu);

        JMenuItem menuItem = new JMenuItem("Load...");
        menuItem.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                final JFileChooser fc = new JFileChooser();
                fc.setFileFilter(new ExtensionFileFilter("XML Files", "xml"));
                int retVal = fc.showOpenDialog(null);
                if (retVal == JFileChooser.APPROVE_OPTION) {
                    File file = fc.getSelectedFile();
                    new ResultParserWorker(file, model).execute();
                }
            }
        });

        menu.add(menuItem);
        frame.setJMenuBar(menuBar);

    }

    public static void main(String[] args) {
        SwingUtilities.invokeLater(new Runnable() {
            public void run() {
                new Main();
            }
        });
    }

    private void createUI() {
        frame = new JFrame("Stabilize Workbench");

        frame.setSize(600, 400);
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        frame.setLocationByPlatform(true);
        frame.setVisible(true);

        Container mainPane = frame.getContentPane();


        cp = new ChartPanel(null);
        mainPane.add(cp, BorderLayout.CENTER);

        final ProbesCheckboxes checkBoxes = new ProbesCheckboxes(model);
        model.addBenchmarkChangeListener(checkBoxes);

        JPanel westPanel = new JPanel();
        westPanel.setLayout(new BorderLayout());

        JPanel nortWestPanel = new JPanel();
        nortWestPanel.setMinimumSize(new Dimension(400, 1));
        nortWestPanel.setLayout(new BoxLayout(nortWestPanel, BoxLayout.Y_AXIS));
        nortWestPanel.add(checkBoxes);
        OpenBenchmarks openBenchmarks = new OpenBenchmarks(model);
        model.addBenchmarkChangeListener(openBenchmarks);

        nortWestPanel.add(openBenchmarks);

        JButton renderButton = new JButton("Render");
        renderButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                updateChart(checkBoxes);
            }
        });
        westPanel.add(nortWestPanel, BorderLayout.NORTH);
        westPanel.add(renderButton, BorderLayout.SOUTH);

        mainPane.add(westPanel, BorderLayout.WEST);

        createChart();
    }

    private void updateChart(ProbesCheckboxes checkBoxes) {
        List<String> selectedProbes = checkBoxes.getEnabledProbes();

        int bins = 60;
        int threshold = 1000;

        MyDataSet dataSet = new MyDataSet();

        Set<String> benchmarkNames = model.getBenchmarkNames();
        for (String benchmarkName : benchmarkNames) {
            BenchmarkResults benchmarkResults = model.getBenchmarkResults(benchmarkName);
            for (String selectedProbe : selectedProbes) {
                Result probeData = benchmarkResults.getProbeData(selectedProbe);
                if (probeData instanceof LatencyDistributionResult) {
                    SimpleHistogramDataset simpleHistogramDataset = new SimpleHistogramDataset("key");
                    String name = benchmarkName + " - " + selectedProbe;
                    dataSet.addNewSeries(simpleHistogramDataset, name);
                    LinearHistogram histogram = ((LatencyDistributionResult) probeData).getHistogram();
                    int[] buckets = histogram.getBuckets();
                    int step = histogram.getStep();
                    int binNo = 0;
                    for (int values : buckets ) {
                        if (values > threshold) {
                            double lowerBound = step * binNo;
                            binNo++;
                            double upperBound = step * binNo;
                            SimpleHistogramBin bin = new SimpleHistogramBin(lowerBound, upperBound, true, false);
                            bin.setItemCount(values);
                            simpleHistogramDataset.addBin(bin);
                        }
                    }
                }
            }
        }
        plot.setDataset(dataSet);
    }

    private void createChart() {
        JFreeChart chart = ChartFactory.createHistogram("Latency Distribution", "Latency (µs)", "Operations", null,
                PlotOrientation.VERTICAL, true, true, true);
        plot = (XYPlot) chart.getPlot();
        plot.setForegroundAlpha(0.65f);
        cp.setChart(chart);

    }

}