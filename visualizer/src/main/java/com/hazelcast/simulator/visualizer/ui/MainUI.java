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

import com.hazelcast.simulator.visualizer.data.Model;
import com.hazelcast.simulator.visualizer.io.ResultParserWorker;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.jfree.ui.ExtensionFileFilter;

import javax.swing.*;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.KeyEvent;
import java.io.File;

public final class MainUI extends JFrame {

    private final Model model = new Model();

    private JMenuItem loadMenuItem;
    private File lastFileChooserDirectory;

    public MainUI() {
        createUI();
        createMenu();

        openLoadMenuItem();
    }

    private void createUI() {
        setTitle("Simulator Workbench");
        setSize(600, 400);
        setDefaultCloseOperation(WindowConstants.EXIT_ON_CLOSE);
        setLocationByPlatform(true);
        setVisible(true);
        setExtendedState(getExtendedState() | JFrame.MAXIMIZED_BOTH);

        AccuracyRadioButtons accuracyRadioButtons = new AccuracyRadioButtons();
        AutoScaleRadioButtons autoScaleRadioButtons = new AutoScaleRadioButtons();

        ProbesCheckboxes checkBoxes = new ProbesCheckboxes(model);
        model.addBenchmarkChangeListener(checkBoxes);

        LoadedBenchmarks loadedBenchmarks = new LoadedBenchmarks();
        model.addBenchmarkChangeListener(loadedBenchmarks);

        JPanel northWestPanel = new JPanel();
        northWestPanel.setLayout(new BoxLayout(northWestPanel, BoxLayout.Y_AXIS));
        northWestPanel.add(accuracyRadioButtons);
        northWestPanel.add(autoScaleRadioButtons);
        northWestPanel.add(checkBoxes);
        northWestPanel.add(loadedBenchmarks);

        final Chart chart = new Chart(model, accuracyRadioButtons, autoScaleRadioButtons, checkBoxes);
        accuracyRadioButtons.setChart(chart);
        autoScaleRadioButtons.setChart(chart);
        checkBoxes.setChart(chart);

        JButton renderButton = new JButton("Reset");
        renderButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                chart.updateChart();
            }
        });

        JPanel westPanel = new JPanel();
        westPanel.setLayout(new BorderLayout());
        westPanel.add(northWestPanel, BorderLayout.NORTH);
        westPanel.add(renderButton, BorderLayout.SOUTH);

        add(chart, BorderLayout.CENTER);
        add(westPanel, BorderLayout.WEST);
    }

    private void createMenu() {
        UIManager.put("FileChooser.readOnly", Boolean.TRUE);

        loadMenuItem = new JMenuItem("Load...");
        loadMenuItem.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                JFileChooser fileChooser = new JFileChooser(lastFileChooserDirectory);
                fileChooser.setFileFilter(new ExtensionFileFilter("XML Files", "xml"));
                int returnValue = fileChooser.showOpenDialog(null);
                if (returnValue == JFileChooser.APPROVE_OPTION) {
                    lastFileChooserDirectory = fileChooser.getSelectedFile();
                    new ResultParserWorker(lastFileChooserDirectory, model).execute();
                }
            }
        });

        JMenuItem exitMenuItem = new JMenuItem("Exit");
        exitMenuItem.addActionListener(new ActionListener() {
            @SuppressFBWarnings({"DM_EXIT"})
            @Override
            public void actionPerformed(ActionEvent e) {
                System.exit(0);
            }
        });

        JMenu menu = new JMenu("File");
        menu.setMnemonic(KeyEvent.VK_A);
        menu.add(loadMenuItem);
        menu.add(exitMenuItem);

        JMenuBar menuBar = new JMenuBar();
        menuBar.add(menu);

        setJMenuBar(menuBar);
    }

    private void openLoadMenuItem() {
        for (ActionListener listener : loadMenuItem.getActionListeners()) {
            listener.actionPerformed(new ActionEvent(this, ActionEvent.ACTION_PERFORMED, null) {
            });
        }
    }
}
