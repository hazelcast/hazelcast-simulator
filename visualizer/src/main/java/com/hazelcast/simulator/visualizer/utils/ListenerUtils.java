/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast.simulator.visualizer.utils;

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
