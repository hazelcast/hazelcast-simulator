package com.hazelcast.simulator.tests.platform.nexmark.model;


import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.StreamSerializer;

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
        public Bid read(ObjectDataInput in) throws IOException {
            long id = in.readLong();
            long timestamp = in.readLong();
            long auctionId = in.readLong();
            long price = in.readLong();
            return new Bid(id, timestamp, auctionId, price);
        }
    }
}
