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
package com.hazelcast.simulator.cluster;

import com.hazelcast.simulator.coordinator.WorkerParameters;
import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.io.xml.DomDriver;

public final class ClusterConfigurationUtils {

    private ClusterConfigurationUtils() {
    }

    public static String toXml(ClusterConfiguration clusterConfiguration) {
        XStream xStream = getXStream(null);
        return xStream.toXML(clusterConfiguration);
    }

    public static ClusterConfiguration fromXml(String xml, WorkerParameters workerParameters) {
        XStream xStream = getXStream(workerParameters);
        return (ClusterConfiguration) xStream.fromXML(xml);
    }

    private static XStream getXStream(WorkerParameters workerParameters) {
        XStream xStream = new XStream(new DomDriver());

        xStream.registerConverter(new WorkerConfigurationConverter(workerParameters));

        xStream.processAnnotations(ClusterConfiguration.class);
        xStream.processAnnotations(WorkerConfiguration.class);
        xStream.processAnnotations(NodeConfiguration.class);
        xStream.processAnnotations(WorkerGroup.class);

        return xStream;
    }
}
