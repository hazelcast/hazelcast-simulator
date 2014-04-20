/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast.stabilizer.agent;

import javax.xml.bind.annotation.XmlRootElement;
import java.io.Serializable;

@XmlRootElement
public class WorkerJvmSettings implements Serializable {
    public String vmOptions;
    public boolean trackLogging;
    public String hzConfig;
    public int workerCount;
    public int workerStartupTimeout;
    public boolean refreshJvm;
    public String javaVendor;
    public String javaVersion;

    @Override
    public String toString() {
        return "WorkerSettings{" +
                "\n  vmOptions='" + vmOptions + '\'' +
                "\n, trackLogging=" + trackLogging +
                "\n, workerJVmCount=" + workerCount +
                "\n, workerStartupTimeout=" + workerStartupTimeout +
                "\n, refreshJvm=" + refreshJvm +
                "\n, javaVendor=" + javaVendor +
                "\n, javaVersion=" + javaVersion +
                "\n, hzConfig='" + hzConfig + '\'' +
                "\n}";
    }
}
