package com.hazelcast.simulator.visualiser.utils;

import javax.swing.*;
import java.awt.*;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;

public final class ListenerUtils {

    private ListenerUtils() {
    }

    public static void addHorizontalMouseListener(final JPanel panel, final JSlider slider) {
        slider.addMouseListener(new MouseAdapter() {
            @Override
            public void mousePressed(MouseEvent e) {
                Point p = e.getPoint();
                double percent = p.x / ((double) panel.getWidth());
                int range = slider.getMaximum() - slider.getMinimum();
                double newVal = range * percent;
                int result = (int) (slider.getMinimum() + newVal);
                slider.setValue(result);
            }
        });
    }

    public static void addVerticalMouseListener(final JPanel panel, final JSlider slider) {
        slider.addMouseListener(new MouseAdapter() {
            @Override
            public void mousePressed(MouseEvent e) {
                Point p = e.getPoint();
                double percent = p.y / ((double) panel.getHeight());
                int range = slider.getMaximum() - slider.getMinimum();
                double newVal = range * percent;
                int result = (int) (slider.getMaximum() - newVal);
                slider.setValue(result);
            }
        });
    }
}
