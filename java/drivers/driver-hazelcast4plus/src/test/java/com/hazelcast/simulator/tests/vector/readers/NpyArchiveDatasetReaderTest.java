package com.hazelcast.simulator.tests.vector.readers;

import com.google.gson.JsonParser;
import com.hazelcast.query.Predicates;
import org.junit.Test;

import static com.hazelcast.simulator.tests.vector.readers.NpyArchiveDatasetReader.parseConditions;
import static org.junit.Assert.assertEquals;

public class NpyArchiveDatasetReaderTest {
    @Test
    public void parseMatchCollection() {
        var pred = parseConditions(JsonParser.parseString("{\"and\": [{\"labels\": {\"match\": {\"value\": \"math.OA\"}}}]}").getAsJsonObject());
        assertEquals(Predicates.equal("labels[any]", "math.OA"), pred);
    }

    @Test
    public void parseMatchPrimitive() {
        var pred = parseConditions(JsonParser.parseString("{\"and\": [{\"submitter\": {\"match\": {\"value\": \"Name Surname\"}}}]}").getAsJsonObject());
        assertEquals(Predicates.equal("submitter", "Name Surname"), pred);
    }

    @Test
    public void parseMatchRange() {
        var pred = parseConditions(JsonParser.parseString("{\"and\": [{\"update_date_ts\": {\"range\": {\"gt\": 1348063655, \"lt\": 1540830205}}}]}").getAsJsonObject());
        assertEquals("(update_date_ts>1348063655 AND update_date_ts<1540830205)", pred.toString());
    }
}
