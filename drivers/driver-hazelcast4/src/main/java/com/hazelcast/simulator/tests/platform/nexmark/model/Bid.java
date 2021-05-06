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

package com.hazelcast.simulator.tests.platform.nexmark.model;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.StreamSerializer;

import javax.annotation.Nonnull;
import java.io.IOException;

public class Bid extends Event {
    private final long auctionId;
    private final long price;

    public Bid(long id, long timestamp, long auctionId, long price) {
        super(id, timestamp);
        this.auctionId = auctionId;
        this.price = price;
    }

    public long auctionId() {
        return auctionId;
    }

    public long price() {
        return price;
    }

    public static class BidSerializer implements StreamSerializer<Bid> {

        @Override
        public int getTypeId() {
            return 2;
        }

        @Override
        public void write(ObjectDataOutput out, Bid bid) throws IOException {
            out.writeLong(bid.id());
            out.writeLong(bid.timestamp());
            out.writeLong(bid.auctionId());
            out.writeLong(bid.price());
        }

        @Override
        @Nonnull
        public Bid read(ObjectDataInput in) throws IOException {
            long id = in.readLong();
            long timestamp = in.readLong();
            long auctionId = in.readLong();
            long price = in.readLong();
            return new Bid(id, timestamp, auctionId, price);
        }
    }
}
