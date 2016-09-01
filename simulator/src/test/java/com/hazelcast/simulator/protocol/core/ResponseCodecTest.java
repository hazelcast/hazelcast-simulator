package com.hazelcast.simulator.protocol.core;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.After;
import org.junit.Test;

import static com.hazelcast.simulator.protocol.core.ResponseCodec.decodeResponse;
import static com.hazelcast.simulator.utils.ReflectionUtils.invokePrivateConstructor;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class ResponseCodecTest {

    private ByteBuf buffer;

    @After
    public void after() {
        if (buffer != null) {
            buffer.release();
        }
    }

    @Test
    public void testConstructor() throws Exception {
        invokePrivateConstructor(ResponseCodec.class);
    }

    @Test(expected = IllegalArgumentException.class)
    public void decodeResponse_invalidMagicBytes() {
        buffer = Unpooled.buffer().capacity(8);
        buffer.writeInt(8);
        buffer.writeInt(0);
        buffer.resetReaderIndex();

        decodeResponse(buffer);
    }

    @Test
    public void encodeDecodeWithoutPayloads(){
        buffer = Unpooled.buffer().capacity(10000);

        SimulatorAddress destination = SimulatorAddress.fromString("C_A1_W1");
        SimulatorAddress source1 = SimulatorAddress.fromString("C_A1");
        SimulatorAddress source2 = SimulatorAddress.fromString("C_A2");
        Response response = new Response(100, destination)
                .addPart(source1, ResponseType.EXCEPTION_DURING_OPERATION_EXECUTION)
                .addPart(source2, ResponseType.SUCCESS);

        ResponseCodec.encodeByteBuf(response,buffer);

        Response found = ResponseCodec.decodeResponse(buffer);
        assertEquals(destination, found.getDestination());
        assertEquals(response.getMessageId(), found.getMessageId());
        assertEquals(2, found.size());

        assertEquals(ResponseType.EXCEPTION_DURING_OPERATION_EXECUTION, found.getPart(source1).getType());
        assertNull(found.getPart(source1).getPayload());

        assertEquals(ResponseType.SUCCESS, found.getPart(source2).getType());
        assertNull(found.getPart(source2).getPayload());
    }

    @Test
    public void encodeDecodeWithPayloads(){
        buffer = Unpooled.buffer().capacity(10000);

        SimulatorAddress destination = SimulatorAddress.fromString("C_A1_W1");
        SimulatorAddress source1 = SimulatorAddress.fromString("C_A1");
        SimulatorAddress source2 = SimulatorAddress.fromString("C_A2");
        Response response = new Response(100, destination)
                .addPart(source1, ResponseType.EXCEPTION_DURING_OPERATION_EXECUTION)
                .addPart(source2, ResponseType.SUCCESS,"success");

        ResponseCodec.encodeByteBuf(response,buffer);

        Response found = ResponseCodec.decodeResponse(buffer);
        assertEquals(destination, found.getDestination());
        assertEquals(response.getMessageId(), found.getMessageId());
        assertEquals(2, found.size());

        assertEquals(ResponseType.EXCEPTION_DURING_OPERATION_EXECUTION, found.getPart(source1).getType());
        assertNull(found.getPart(source1).getPayload());

        assertEquals(ResponseType.SUCCESS, found.getPart(source2).getType());
        assertEquals("success", found.getPart(source2).getPayload());
    }
}
