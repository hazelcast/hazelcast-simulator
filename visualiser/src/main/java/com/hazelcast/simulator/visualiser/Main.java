package com.hazelcast.simulator.visualiser;

import com.hazelcast.simulator.visualiser.data.Model;
import com.hazelcast.simulator.visualiser.io.ResultParserWorker;
import com.hazelcast.simulator.visualiser.ui.Chart;
import com.hazelcast.simulator.visualiser.ui.LoadedBenchmarks;
import com.hazelcast.simulator.visualiser.ui.ProbesCheckboxes;
import org.jfree.ui.ExtensionFileFilter;

import javax.swing.*;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.KeyEvent;
import java.io.File;

public class Main {

    private final Model model;
    private final JFrame frame;

    public Main() {
        model = new Model();
        frame = new JFrame("Simulator Workbench");

        createUI();
        createMenu();
    }

    private void createUI() {
        frame.setSize(600, 400);
        frame.setDefaultCloseOperation(WindowConstants.EXIT_ON_CLOSE);
        frame.setLocationByPlatform(true);
        frame.setVisible(true);
        frame.setDefaultCloseOperation(WindowConstants.EXIT_ON_CLOSE);

        Container mainPane = frame.getContentPane();

        ProbesCheckboxes checkBoxes = new ProbesCheckboxes(model);
        model.addBenchmarkChangeListener(checkBoxes);

        final Chart chartPanel = new Chart(model, checkBoxes);
        mainPane.add(chartPanel, BorderLayout.CENTER);

        JPanel westPanel = new JPanel();
        westPanel.setLayout(new BorderLayout());

        JPanel northWestPanel = new JPanel();
        //northWestPanel.setMinimumSize(new Dimension(400, 1));
        northWestPanel.setLayout(new BoxLayout(northWestPanel, BoxLayout.Y_AXIS));
        northWestPanel.add(checkBoxes);
        LoadedBenchmarks loadedBenchmarks = new LoadedBenchmarks();
        model.addBenchmarkChangeListener(loadedBenchmarks);

        northWestPanel.add(loadedBenchmarks);

        JButton renderButton = new JButton("Render");
        renderButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                chartPanel.updateChart();
            }
        });
        westPanel.add(northWestPanel, BorderLayout.NORTH);
        westPanel.add(renderButton, BorderLayout.SOUTH);

        mainPane.add(westPanel, BorderLayout.WEST);
    }

    private void createMenu() {
        JMenuBar menuBar = new JMenuBar();
        JMenu menu = new JMenu("File");
        menu.setMnemonic(KeyEvent.VK_A);
        menuBar.add(menu);

        JMenuItem loadMenuItem = new JMenuItem("Load...");
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

        menu.add(loadMenuItem);
        menu.add(exitMenuItem);
        frame.setJMenuBar(menuBar);
    }

    public static void main(String[] args) {
        SwingUtilities.invokeLater(new Runnable() {
            public void run() {
                new Main();
            }
        });
    }
}