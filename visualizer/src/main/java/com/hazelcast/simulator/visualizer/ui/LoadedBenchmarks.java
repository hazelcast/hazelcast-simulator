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
package com.hazelcast.simulator.visualizer.ui;

import com.hazelcast.simulator.visualizer.data.Model;

import javax.swing.*;
import java.awt.*;

public class LoadedBenchmarks extends JPanel implements Model.BenchmarkChangeListener {

    public LoadedBenchmarks() {
        setMaximumSize(new Dimension(Integer.MAX_VALUE, Integer.MAX_VALUE));
        setBorder(BorderFactory.createTitledBorder("Benchmarks"));
        setLayout(new BoxLayout(this, BoxLayout.Y_AXIS));
    }

    @Override
    public void benchmarkChanged(String benchmarkName) {
        JLabel label = new JLabel(benchmarkName);
        add(label);
    }
}
