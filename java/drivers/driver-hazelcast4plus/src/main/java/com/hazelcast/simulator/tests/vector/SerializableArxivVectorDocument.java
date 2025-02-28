package com.hazelcast.simulator.tests.vector;

import com.google.gson.Gson;
import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.vector.VectorDocument;
import com.hazelcast.vector.VectorValues;

import javax.annotation.Nonnull;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Objects;

/**
 * Vector document with Arxiv record value. Uses java serialization for simplicity.
 * With inMemoryFormat=OBJECT is will be serialized mostly during ingestion
 * and to pass around intermediate and final query results, but not during
 * the most time-consuming parts of the query.
 */
public final class SerializableArxivVectorDocument implements VectorDocument<HazelcastJsonValue>, Serializable {

    private final static Gson gson = new Gson();

    private int update_date_ts;
    private String[] labels;
    private String submitter;
    private String id;
    private float[] vector;

    public static SerializableArxivVectorDocument of(HazelcastJsonValue value, @Nonnull float[] vector) {
        var obj = gson.fromJson(value.getValue(), SerializableArxivVectorDocument.class);
        obj.vector = vector;
        return obj;
    }

    @Nonnull
    @Override
    public HazelcastJsonValue getValue() {
        return null;
    }

    @Nonnull
    @Override
    public VectorValues getVectors() {
        return VectorValues.of(vector);
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SerializableArxivVectorDocument that = (SerializableArxivVectorDocument) o;
        return update_date_ts == that.update_date_ts && Objects.deepEquals(labels, that.labels)
                && Objects.equals(submitter, that.submitter) && Objects.equals(id, that.id)
                && Objects.deepEquals(vector, that.vector);
    }

    @Override
    public int hashCode() {
        return Objects.hash(update_date_ts, Arrays.hashCode(labels), submitter, id, Arrays.hashCode(vector));
    }

    @Override
    public String toString() {
        return "SerializableArxivVectorDocument{" +
                "update_date_ts=" + update_date_ts +
                ", labels=" + Arrays.toString(labels) +
                ", submitter='" + submitter + '\'' +
                ", id='" + id + '\'' +
                '}';
    }
}
