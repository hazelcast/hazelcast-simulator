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
package com.hazelcast.simulator.tests.network;

import com.hazelcast.nio.Packet;
import com.hazelcast.nio.tcp.WriteHandler;

import java.nio.ByteBuffer;

import static com.hazelcast.nio.Packet.HEADER_BIND;
import static com.hazelcast.simulator.tests.network.PayloadUtils.addSequenceId;

/**
 * a WriteHandler that at the beginning and end of the payload inserts a sequence-id.
 *
 * This sequence-id is unique per connection and is totally ordered. So the receiver of these packets should
 * get an incremental stream of sequence-id's.
 */
class TaggingPacketWriteHandler implements WriteHandler<Packet> {

    // we keep track of the current packet because we need to know if the packet was already tagged before.
    // it can be that a packet is offered to the WriteHandler more than once if it can't be fully written
    // to the bb. If we would not protect ourselves against that, things get funny because the sequenceId is
    // incremented multiple times and the sequence in the packets could be a mixture of sequence-id.
    private Packet currentPacket;
    private long sequenceId = 1;

    @Override
    public boolean onWrite(Packet packet, ByteBuffer dst) throws Exception {
        if (currentPacket == null && !packet.isHeaderSet(HEADER_BIND) && packet.dataSize() > 100) {
            currentPacket = packet;
            addSequenceId(packet.toByteArray(), sequenceId);
            sequenceId++;
        }

        boolean completed = packet.writeTo(dst);
        if (completed) {
            currentPacket = null;
        }
        return completed;
    }
}
