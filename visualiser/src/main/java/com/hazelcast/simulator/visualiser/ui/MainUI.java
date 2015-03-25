package com.hazelcast.simulator.visualiser.ui;

import com.hazelcast.simulator.visualiser.data.Model;
import com.hazelcast.simulator.visualiser.io.ResultParserWorker;
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

        ProbesCheckboxes checkBoxes = new ProbesCheckboxes(model);
        model.addBenchmarkChangeListener(checkBoxes);

        LoadedBenchmarks loadedBenchmarks = new LoadedBenchmarks();
        model.addBenchmarkChangeListener(loadedBenchmarks);

        JPanel northWestPanel = new JPanel();
        northWestPanel.setLayout(new BoxLayout(northWestPanel, BoxLayout.Y_AXIS));
        northWestPanel.add(checkBoxes);
        northWestPanel.add(loadedBenchmarks);

        final Chart chart = new Chart(model, checkBoxes);
        checkBoxes.setChart(chart);

        JButton renderButton = new JButton("Render");
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
                JFileChooser fc = new JFileChooser();
                fc.setFileFilter(new ExtensionFileFilter("XML Files", "xml"));
                int retVal = fc.showOpenDialog(null);
                if (retVal == JFileChooser.APPROVE_OPTION) {
                    File file = fc.getSelectedFile();
                    new ResultParserWorker(file, model).execute();
                }
            }
        });

        JMenuItem exitMenuItem = new JMenuItem("Exit");
        exitMenuItem.addActionListener(new ActionListener() {
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
