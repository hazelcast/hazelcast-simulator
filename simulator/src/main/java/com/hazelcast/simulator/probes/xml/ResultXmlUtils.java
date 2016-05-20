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
package com.hazelcast.simulator.probes.xml;

import com.hazelcast.simulator.probes.Result;
import com.hazelcast.simulator.probes.impl.ResultImpl;
import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.converters.extended.NamedMapConverter;
import com.thoughtworks.xstream.io.xml.DomDriver;
import com.thoughtworks.xstream.mapper.Mapper;
import org.HdrHistogram.Histogram;

import java.io.File;

import static com.hazelcast.simulator.utils.FileUtils.writeText;

public final class ResultXmlUtils {

    private ResultXmlUtils() {
    }

    public static String toXml(Result result) {
        XStream xStream = getXStream();
        return xStream.toXML(result);
    }

    public static void toXml(Result result, File file) {
        writeText(toXml(result), file);
    }

    public static Result fromXml(File file) {
        XStream xStream = getXStream();
        return (Result) xStream.fromXML(file);
    }

    private static XStream getXStream() {
        XStream xStream = new XStream(new DomDriver());
        Mapper mapper = xStream.getMapper();

        xStream.registerConverter(new HistogramConverter());
        xStream.registerConverter(new NamedMapConverter(mapper, null, "name", String.class, "data", Histogram.class));

        xStream.alias("probeResult", ResultImpl.class);
        xStream.alias("histogram", Histogram.class);
        xStream.aliasField("histograms", ResultImpl.class, "probeHistogramMap");

        return xStream;
    }
}
