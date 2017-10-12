/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast.simulator.heatmap;

import java.io.File;
import java.io.FilenameFilter;

class HistogramFilenameFilter implements FilenameFilter {

    private final String aggregatedFileRegex;
    private final String latencyFileRegex;

    HistogramFilenameFilter(String testName, String probeName) {
        testName = (!testName.isEmpty()) ? testName : ".*";
        probeName = (!probeName.isEmpty()) ? probeName : testName + "WorkerProbe";

        this.aggregatedFileRegex = "^latency-" + testName + "-aggregated\\.txt$";
        this.latencyFileRegex = "^latency-" + testName + "-" + probeName + "\\.txt$";
    }

    @Override
    public boolean accept(File dir, String name) {
        File file = new File(dir, name);
        if (file.isDirectory()) {
            return true;
        }
        if (name.matches(aggregatedFileRegex)) {
            return false;
        }
        if (!name.matches(latencyFileRegex)) {
            return false;
        }
        return true;
    }
}
