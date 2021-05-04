package com.hazelcast.simulator.tests.platform.nexmark.model;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.StreamSerializer;

import java.io.IOException;

public class Auction extends Event {
    private final long sellerId;
    private final int category;
    private final long expires;

    public Auction(long id, long timestamp, long sellerId, int category, long expires) {
        super(id, timestamp);
        this.sellerId = sellerId;
        this.category = category;
        this.expires = expires;
    }

    public long sellerId() {
        return sellerId;
    }

    public int category() {
        return category;
    }

    public long expires() {
        return expires;
    }

    public static class AuctionSerializer implements StreamSerializer<Auction> {

        @Override
        public int getTypeId() {
            return 1;
        }

        @Override
        public void write(ObjectDataOutput out, Auction auction) throws IOException {
            out.writeLong(auction.id());
            out.writeLong(auction.timestamp());
            out.writeLong(auction.sellerId());
            out.writeInt(auction.category());
            out.writeLong(auction.expires());
        }

        @Override
        public Auction read(ObjectDataInput in) throws IOException {
            long id = in.readLong();
            long timestamp = in.readLong();
            long sellerId = in.readLong();
            int category = in.readInt();
            long expires = in.readLong();

            return new Auction(id, timestamp, sellerId, category, expires);
        }
    }
}
