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
package com.hazelcast.simulator.visualizer.io;

import com.hazelcast.simulator.probes.Result;
import com.hazelcast.simulator.probes.xml.ResultXmlUtils;
import com.hazelcast.simulator.visualizer.data.Model;

import javax.swing.*;
import java.io.File;
import java.util.concurrent.ExecutionException;

public class ResultParserWorker extends SwingWorker<Result, Void> {

    private final File file;
    private final Model model;

    public ResultParserWorker(File file, Model model) {
        this.file = file;
        this.model = model;
    }

    @Override
    protected Result doInBackground() throws Exception {
        return ResultXmlUtils.fromXml(file);
    }

    @Override
    protected void done() {
        try {
            Result result = get();
            model.addResults(result);
        } catch (InterruptedException e) {
            throw new ResultParserException(e);
        } catch (ExecutionException e) {
            throw new ResultParserException(e);
        }
    }
}
