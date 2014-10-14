package com.hazelcast.stabilizer.visualiser;

import com.hazelcast.stabilizer.visualiser.data.Model;
import com.hazelcast.stabilizer.visualiser.io.ResultParserWorker;
import com.hazelcast.stabilizer.visualiser.ui.Chart;
import com.hazelcast.stabilizer.visualiser.ui.LoadedBenchmarks;
import com.hazelcast.stabilizer.visualiser.ui.ProbesCheckboxes;
import org.jfree.ui.ExtensionFileFilter;

import javax.swing.*;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.KeyEvent;
import java.io.File;

public class Main {

    private JFrame frame;
    private Chart chartPanel;
    private Model model;

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

        JMenuItem loadMenuItem = new JMenuItem("Load...");
        loadMenuItem.addActionListener(new ActionListener() {
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

    private void createUI() {
        frame = new JFrame("Stabilizer Workbench");

        frame.setSize(600, 400);
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        frame.setLocationByPlatform(true);
        frame.setVisible(true);
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);

        Container mainPane = frame.getContentPane();



        final ProbesCheckboxes checkBoxes = new ProbesCheckboxes(model);
        model.addBenchmarkChangeListener(checkBoxes);

        chartPanel = new Chart(model, checkBoxes);
        mainPane.add(chartPanel, BorderLayout.CENTER);

        JPanel westPanel = new JPanel();
        westPanel.setLayout(new BorderLayout());

        JPanel nortWestPanel = new JPanel();
//        nortWestPanel.setMinimumSize(new Dimension(400, 1));
        nortWestPanel.setLayout(new BoxLayout(nortWestPanel, BoxLayout.Y_AXIS));
        nortWestPanel.add(checkBoxes);
        LoadedBenchmarks loadedBenchmarks = new LoadedBenchmarks();
        model.addBenchmarkChangeListener(loadedBenchmarks);

        nortWestPanel.add(loadedBenchmarks);

        JButton renderButton = new JButton("Render");
        renderButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                chartPanel.updateChart();
            }
        });
        westPanel.add(nortWestPanel, BorderLayout.NORTH);
        westPanel.add(renderButton, BorderLayout.SOUTH);

        mainPane.add(westPanel, BorderLayout.WEST);

    }
}