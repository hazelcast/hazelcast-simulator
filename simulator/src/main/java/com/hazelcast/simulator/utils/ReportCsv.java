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
package com.hazelcast.simulator.utils;

import java.io.File;
import java.util.LinkedList;
import java.util.Queue;

import static com.hazelcast.simulator.utils.FileUtils.appendText;
import static com.hazelcast.simulator.utils.FileUtils.fileAsText;
import static com.hazelcast.simulator.utils.FileUtils.stripExtension;
import static java.util.Arrays.asList;

@SuppressWarnings("checkstyle:magicnumber")
public final class ReportCsv {

    private ReportCsv() {
    }

    public static void main(String[] args) {
        File hgrmFile = new File(args[0]);
        File reportDir = new File(args[1]);
        File sessionFile = new File(args[2]);
        File out = new File(reportDir, "report.csv");
        if (!out.exists()) {
            FileUtils.writeText(getHeader(), out);
        }

        StringBuffer outSb = new StringBuffer();
        outSb.append(sessionFile.getName());
        outSb.append(",").append(stripExtension(hgrmFile.getName()));
        addPercentiles(hgrmFile, outSb);
        addOther(hgrmFile, outSb);
        appendText(outSb.toString() + "\n", out);
    }

    private static void addPercentiles(File hgrmFile, StringBuffer outSb) {
        String[] lines = fileAsText(hgrmFile).split("\n");
        Queue<String> importantPercentiles = new LinkedList<>(asList("0.1", "0.2", "0.5", "0.75", "0.90", "0.95",
                "0.99", "0.999", "0.9999", "1.00"));

        String percentile = importantPercentiles.poll();
        for (int k = 4; k < lines.length - 3; k++) {
            String line = lines[k];
            String[] tokens = line.split("\\s+");
            if (tokens[2].startsWith(percentile)) {
                outSb.append(',').append(tokens[1]);
                percentile = importantPercentiles.poll();
                if (percentile == null) {
                    break;
                }
            }
        }
    }

    private static void addOther(File hgrmFile, StringBuffer outSb) {
        File file = new File(hgrmFile.getParent(), FileUtils.stripExtension(hgrmFile.getName()));
        String[] lines = fileAsText(file).split("\n");
        long startMillis = Math.round(Double.parseDouble(lines[4].split(",")[1]) * 1000);

        String lastLine = lines[lines.length - 1];

        String[] lastLineFields = lastLine.split(",");
        long totalCount = Long.parseLong(lastLineFields[16]);
        outSb.append(",").append(totalCount);

        long endMillis = Math.round(Double.parseDouble(lastLineFields[1]) * 1000);
        long durationMillis = endMillis - startMillis;
        outSb.append(",").append(durationMillis);

        outSb.append(",").append(totalCount * 1000d / durationMillis);
    }

    private static String getHeader() {
        return "\"session\",\"benchmark\",\"10%\",\"20%\",\"50%\",\"75%\",\"90%\",\"95%\",\"99%\",\"99.9%\","
                + "\"99.99%\",\"max\",\"operations\",\"duration_ms\",\"throughput\"\n";
    }
}
