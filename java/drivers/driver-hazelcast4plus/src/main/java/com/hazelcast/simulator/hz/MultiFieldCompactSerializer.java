package com.hazelcast.simulator.hz;

import com.hazelcast.nio.serialization.compact.CompactReader;
import com.hazelcast.nio.serialization.compact.CompactSerializer;
import com.hazelcast.nio.serialization.compact.CompactWriter;
import org.jetbrains.annotations.NotNull;

public class MultiFieldCompactSerializer implements CompactSerializer<MultiFieldCompactPojo> {
    @NotNull
    @Override
    public MultiFieldCompactPojo read(@NotNull CompactReader compactReader) {
        MultiFieldCompactPojo pojo = new MultiFieldCompactPojo();
        pojo.str1 = compactReader.readString("str1");
        pojo.str2 = compactReader.readString("str2");
        pojo.str3 = compactReader.readString("str3");
        pojo.str4 = compactReader.readString("str4");
        pojo.str5 = compactReader.readString("str5");

        pojo.int1 = compactReader.readInt32("int1");
        pojo.int2 = compactReader.readInt32("int2");
        pojo.int3 = compactReader.readInt32("int3");
        pojo.int4 = compactReader.readInt32("int4");
        pojo.int5 = compactReader.readInt32("int5");

        pojo.long1 = compactReader.readInt64("long1");
        pojo.long2 = compactReader.readInt64("long2");
        pojo.long3 = compactReader.readInt64("long3");
        pojo.long4 = compactReader.readInt64("long4");
        pojo.long5 = compactReader.readInt64("long5");

        pojo.bool1 = compactReader.readBoolean("bool1");
        pojo.bool2 = compactReader.readBoolean("bool2");
        pojo.bool3 = compactReader.readBoolean("bool3");
        pojo.bool4 = compactReader.readBoolean("bool4");
        pojo.bool5 = compactReader.readBoolean("bool5");

        return pojo;
    }

    @Override
    public void write(@NotNull CompactWriter compactWriter, @NotNull MultiFieldCompactPojo pojo) {
        compactWriter.writeString("str1", pojo.str1 );
        compactWriter.writeString("str2", pojo.str2 );
        compactWriter.writeString("str3", pojo.str3 );
        compactWriter.writeString("str4", pojo.str4 );
        compactWriter.writeString("str5", pojo.str5 );

        compactWriter.writeInt32("int1", pojo.int1 );
        compactWriter.writeInt32("int2", pojo.int2 );
        compactWriter.writeInt32("int3", pojo.int3 );
        compactWriter.writeInt32("int4", pojo.int4 );
        compactWriter.writeInt32("int5", pojo.int5 );

        compactWriter.writeInt64("long1", pojo.long1 );
        compactWriter.writeInt64("long2", pojo.long2 );
        compactWriter.writeInt64("long3", pojo.long3 );
        compactWriter.writeInt64("long4", pojo.long4 );
        compactWriter.writeInt64("long5", pojo.long5 );

        compactWriter.writeBoolean("bool1", pojo.bool1 );
        compactWriter.writeBoolean("bool2", pojo.bool2 );
        compactWriter.writeBoolean("bool3", pojo.bool3 );
        compactWriter.writeBoolean("bool4", pojo.bool4 );
        compactWriter.writeBoolean("bool5", pojo.bool5 );
    }

    @NotNull
    @Override
    public String getTypeName() {
        return "multiFieldCompactPojo";
    }

    @NotNull
    @Override
    public Class<MultiFieldCompactPojo> getCompactClass() {
        return MultiFieldCompactPojo.class;
    }
}
