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
package com.hazelcast.simulator.tests.syntheticmap;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.AbstractOperation;
import com.hazelcast.spi.PartitionAwareOperation;

import java.io.IOException;

public class GetOperation extends AbstractOperation implements PartitionAwareOperation, IdentifiedDataSerializable {

    private String mapName;
    private Data key;
    private Data response;

    public GetOperation() {
    }

    public GetOperation(String mapName, Data key) {
        this.mapName = mapName;
        this.key = key;
    }

    @Override
    public String getServiceName() {
        return SyntheticMapService.SERVICE_NAME;
    }

    @Override
    public void run() throws Exception {
        SyntheticMapService mapService = getService();
        response = mapService.get(getPartitionId(), mapName, key);
    }

    @Override
    public Data getResponse() {
        return response;
    }

    @Override
    public int getFactoryId() {
        return SyntheticMapSerializableFactory.ID;
    }

    @Override
    public int getId() {
        return SyntheticMapSerializableFactory.GET_OPERATION;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeUTF(mapName);
        out.writeData(key);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        mapName = in.readUTF();
        key = in.readData();
    }
}
