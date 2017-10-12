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
import java.util.ArrayList;
import java.util.List;

class FileWalker {

    private final List<File> getFiles = new ArrayList<File>();
    private final FilenameFilter filenameFilter;

    FileWalker(FilenameFilter filenameFilter) {
        this.filenameFilter = filenameFilter;
    }

    void walk(File path) {
        File[] list = path.listFiles(filenameFilter);
        if (list == null) {
            return;
        }
        for (File file : list) {
            if (file.isDirectory()) {
                walk(file);
            } else {
                getFiles.add(file);
            }
        }
    }

    List<File> getGetFiles() {
        return getFiles;
    }
}
