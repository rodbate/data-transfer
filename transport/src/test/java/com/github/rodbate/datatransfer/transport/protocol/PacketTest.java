package com.github.rodbate.datatransfer.transport.protocol;

import com.github.rodbate.datatransfer.transport.BaseTest;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * User: rodbate
 * Date: 2018/12/13
 * Time: 16:53
 */
public class PacketTest extends BaseTest {

    @Test
    public void testCodec_withoutExtFields_withoutBoy() {
        Packet packet = Packet.createRequestPacket(1);
        Packet decode = decodeFromFullPacket(packet.encode());
        assertEquals(packet, decode);
    }

    @Test
    public void testCodec_withExtFields_withoutBody() {
        Packet packet = Packet.createRequestPacket(1);
        Map<String, String> extFields = new HashMap<>();
        extFields.put("a", "10");
        extFields.put("b", "100");
        extFields.put("c", "200");
        packet.setExtFields(extFields);

        Packet decode = decodeFromFullPacket(packet.encode());
        assertEquals(packet, decode);
    }

    @Test
    public void testCodec_withoutExtFields_withBody() {
        Packet packet = Packet.createRequestPacket(1);
        packet.setBody("packet body !!".getBytes(StandardCharsets.UTF_8));
        Packet decode = decodeFromFullPacket(packet.encode());
        assertEquals(packet, decode);
    }

    @Test
    public void testCodec_withExtFields_withBody() {
        Packet packet = Packet.createRequestPacket(1);
        Map<String, String> extFields = new HashMap<>();
        extFields.put("a", "10");
        extFields.put("b", "100");
        extFields.put("c", "200");
        packet.setExtFields(extFields);
        packet.setBody("packet body !!".getBytes(StandardCharsets.UTF_8));
        Packet decode = decodeFromFullPacket(packet.encode());
        assertEquals(packet, decode);
    }

    @Test
    public void testCodec_withExtFields_withBody_withRemark() {
        Packet packet = Packet.createRequestPacket(1);
        Map<String, String> extFields = new HashMap<>();
        extFields.put("a", "10");
        extFields.put("b", "100");
        extFields.put("c", "200");
        packet.setExtFields(extFields);
        packet.setRemark("test remark 中文！");
        packet.setBody("packet body !!".getBytes(StandardCharsets.UTF_8));
        Packet decode = decodeFromFullPacket(packet.encode());
        assertEquals(packet, decode);
    }


    @Test
    public void testCodec_withoutExtFields_withoutBoy_withRemark() {
        Packet packet = Packet.createRequestPacket(1);
        packet.setRemark("test remark 中文测试！@#!$");
        Packet decode = decodeFromFullPacket(packet.encode());
        assertEquals(packet, decode);
    }


    private Packet decodeFromFullPacket(ByteBuffer fullPacket) {
        fullPacket.getInt();
        return Packet.decode(fullPacket);
    }

    private void assertEquals(final Packet expected, final Packet target) {
        assertArrayEquals(expected.getBody(), target.getBody());
        assertEquals(expected.getCode(), target.getCode());
        assertEquals(expected.getFlag(), target.getFlag());
        assertEquals(expected.getRemark(), target.getRemark());
        assertEquals(expected.getSeqId(), target.getSeqId());
        assertEquals(expected.getVersion(), target.getVersion());
        assertEquals(expected.getExtFields(), target.getExtFields());
    }

}
