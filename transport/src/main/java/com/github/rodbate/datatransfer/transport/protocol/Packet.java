package com.github.rodbate.datatransfer.transport.protocol;

import com.github.rodbate.datatransfer.transport.netty.systemcode.NettyTransportSystemResponseCode;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 *
 *   packet structure
 *                                  |                                           header                                                                                      |
 * packet length     header length |   version    seq id     command code  flag(request/response)    remark length   remark data  extension fields length extension fields |
 *     4bytes            2byte    |    1byte       4bytes         4byte             1byte                2byte        n bytes              2byte                          |        packet body
 * |--------------|--------------|-----------|-------------|---------------|-----------------------|------------|--------------|-----------------------|-----------------|-------------\\\---------------|
 *
 *
 * User: rodbate
 * Date: 2018/12/13
 * Time: 14:32
 */
@Getter
@Setter
public class Packet {

    private static final AtomicInteger CORRELATION_ID = new AtomicInteger(1);
    private static final byte REQUEST = 0;
    private static final byte RESPONSE = 1;
    private static final byte ONE_WAY_REQUEST = 1 << 1;

    /**
     *  packet version
     */
    private byte version = ProtocolVersion.V1;

    /**
     * seq id
     */
    private int seqId;

    /**
     *  request command code
     */
    private int code;

    /**
     *  flag request or response
     */
    private byte flag = REQUEST;

    /**
     * remark
     */
    private String remark;

    /**
     *  extension fields
     */
    private Map<String, String> extFields;

    /**
     *  packet body
     */
    private byte[] body;


    /**
     * packet size
     */
    private int packetSize;


    /**
     * encode packet to bytes
     *
     * @return bytes data
     */
    public ByteBuffer encode() {
        ByteBuffer buf = encodeHeader();
        if (this.body != null && this.body.length > 0) {
            ByteBuffer data = ByteBuffer.allocate(buf.remaining() + this.body.length);
            data.put(buf);
            data.put(this.body);
            data.flip();
            buf = data;
        }
        return buf;
    }


    /**
     * encode packet header to bytes
     *
     * @return bytes data
     */
    public ByteBuffer encodeHeader() {
        int packetLength = 0;

        //header length field(2bytes)
        packetLength += 2;

        //encode header
        ByteBuffer headerData = encodeHeader0();
        packetLength += headerData.remaining();

        //packet body
        if (this.body != null) {
            packetLength += this.body.length;
        }

        final ByteBuffer buf = ByteBuffer.allocate(packetLength + 4);

        //1. packet length
        buf.putInt(packetLength);

        //2. header length 2bytes
        buf.putShort((short) headerData.remaining());

        //3. header data
        buf.put(headerData);
        buf.flip();
        return buf;
    }


    private ByteBuffer encodeHeader0() {
        int headerLength = 0;

        //1. version 1byte
        headerLength += 1;
        //2. seq id 4bytes
        headerLength += 4;
        //3. command code 4btyes
        headerLength += 4;
        //4. flag 1byte
        headerLength += 1;
        //5. remark len 2bytes
        headerLength += 2;
        //6. remark data
        byte[] remarkData = new byte[0];
        if (StringUtils.isNotBlank(this.remark)) {
            remarkData = this.remark.getBytes(StandardCharsets.UTF_8);
            headerLength += remarkData.length;
        }
        //7. ext fields length len 2bytes
        headerLength += 2;
        //8. ext fields
        byte[] extFieldsData = encodeExtFields();
        headerLength += extFieldsData.length;

        if (headerLength > Short.MAX_VALUE) {
            throw new RuntimeException(String.format("header length[%d] exceed short max value[%d]", headerLength, Short.MAX_VALUE));
        }

        //encode header
        final ByteBuffer buf = ByteBuffer.allocate(headerLength);

        //1. version 1byte
        buf.put(this.version);
        //2. seq id  4bytes
        buf.putInt(this.seqId);
        //3. command code 4byts
        buf.putInt(this.code);
        //4. flag 1byte
        buf.put(this.flag);
        //5. remark
        buf.putShort((short) remarkData.length);
        buf.put(remarkData);
        //6. ext fields
        buf.putShort((short) extFieldsData.length);
        buf.put(extFieldsData);
        buf.flip();
        return buf;
    }


    private byte[] encodeExtFields() {
        if (this.extFields == null || this.extFields.size() == 0) {
            return new byte[0];
        }
        //calculate ex fields total length
        int extFieldsLength = 0;
        String key;
        String value;
        for (Map.Entry<String, String> entry : this.extFields.entrySet()) {
            key = entry.getKey();
            value = entry.getValue();

            // map key
            extFieldsLength = extFieldsLength + 2 + key.getBytes(StandardCharsets.UTF_8).length;

            //map value
            extFieldsLength = extFieldsLength + 2 + value.getBytes(StandardCharsets.UTF_8).length;
        }

        if (extFieldsLength > Short.MAX_VALUE) {
            throw new RuntimeException(String.format("ext fields length[%d] exceed short max value[%d]", extFieldsLength, Short.MAX_VALUE));
        }

        //encode ext fields
        final ByteBuffer buf = ByteBuffer.allocate(extFieldsLength);
        byte[] keyData;
        byte[] valueData;
        for (Map.Entry<String, String> entry : this.extFields.entrySet()) {
            keyData = entry.getKey().getBytes(StandardCharsets.UTF_8);
            valueData = entry.getValue().getBytes(StandardCharsets.UTF_8);

            // map key
            buf.putShort((short) keyData.length);
            buf.put(keyData);

            //map value
            buf.putShort((short) valueData.length);
            buf.put(valueData);
        }
        return buf.array();
    }


    /**
     * whether is request or not
     *
     * @return true if request else false
     */
    public boolean isResponse() {
        return (this.flag & RESPONSE) == RESPONSE;
    }

    /**
     * mark this packet response
     */
    public void markResponse() {
        this.flag |= RESPONSE;
    }

    /**
     * one way request or not, used as heartbeat etc..
     *
     * @return true one way request else false
     */
    public boolean isOneWayRequest() {
        return !isResponse() && ((this.flag & ONE_WAY_REQUEST) == ONE_WAY_REQUEST);
    }

    /**
     * set this packet one way request
     */
    public void markOneWayRequest() {
        this.flag &= ~RESPONSE;
        this.flag |= ONE_WAY_REQUEST;
    }

    /**
     * whether success response
     * @return true if success response else false
     */
    public boolean isSuccessResponse() {
        return isResponse() && this.code == NettyTransportSystemResponseCode.SUCCESS;
    }

    /**
     * decode bytes to packet
     *
     * @param data bytes
     * @return packet
     */
    public static Packet decode(final ByteBuffer data) {
        if (data == null) {
            return null;
        }
        final Packet packet = new Packet();

        int packetLength = data.remaining();
        packet.setPacketSize(packetLength + 4);

        //header length
        int headerLength = data.getShort();
        byte[] headerData = new byte[headerLength];
        data.get(headerData);

        //decode header
        decodeHeader(headerData, packet);

        //body
        int bodyLength = packetLength - 2 - headerLength;
        if (bodyLength > 0) {
            byte[] body = new byte[bodyLength];
            data.get(body);
            packet.setBody(body);
        }
        return packet;
    }


    private static void decodeHeader(final byte[] headerData, final Packet packet) {
        final ByteBuffer buf = ByteBuffer.wrap(headerData);

        //1. version 1byte
        packet.setVersion(buf.get());

        //2. seq id 4bytes
        packet.setSeqId(buf.getInt());

        //3. command code 4bytes
        packet.setCode(buf.getInt());

        //4. flag 1byte
        packet.setFlag(buf.get());

        //5. remark
        short remarkLength = buf.getShort();
        if (remarkLength > 0) {
            byte[] remarkData = new byte[remarkLength];
            buf.get(remarkData);
            packet.setRemark(new String(remarkData, StandardCharsets.UTF_8));
        }

        //6. extension fields
        short extFieldsLength = buf.getShort();
        if (extFieldsLength > 0) {
            byte[] extFieldsData = new byte[extFieldsLength];
            buf.get(extFieldsData);
            packet.setExtFields(decodeExtFields(extFieldsData));
        }
    }


    private static Map<String, String> decodeExtFields(final byte[] extFieldsData) {
        final Map<String, String> extFields = new HashMap<>();

        //<key length 2bytes><key bytes data><value length 2bytes><value bytes data>
        final ByteBuffer buf = ByteBuffer.wrap(extFieldsData);
        short keyLen;
        short valueLen;
        while (buf.hasRemaining()) {
            //map key
            keyLen = buf.getShort();
            byte[] key = new byte[keyLen];
            buf.get(key);

            //map value
            valueLen = buf.getShort();
            byte[] value = new byte[valueLen];
            buf.get(value);

            extFields.put(new String(key, StandardCharsets.UTF_8), new String(value, StandardCharsets.UTF_8));
        }
        return extFields;
    }


    /**
     * create request packet
     *
     * @param code command code
     * @return packet
     */
    public static Packet createRequestPacket(final int code) {
        Packet packet = new Packet();
        packet.setCode(code);
        packet.setSeqId(CORRELATION_ID.getAndIncrement());
        return packet;
    }


    /**
     * create response packet
     *
     * @param code  command code
     * @return packet
     */
    public static Packet createResponsePacket(final int code) {
        Packet packet = new Packet();
        packet.setCode(code);
        packet.markResponse();
        return packet;
    }

    /**
     * create success response packet
     *
     * @return packet
     */
    public static Packet createSuccessResponsePacket() {
        return createResponsePacket(NettyTransportSystemResponseCode.SUCCESS);
    }

    @Override
    public String toString() {
        return "Packet{" +
            "version=" + version +
            ", packetSize=" + packetSize +
            ", seqId=" + seqId +
            ", code=" + code +
            ", flag=" + flag +
            ", remark='" + remark + '\'' +
            ", extFields=" + extFields +
            ", body=bytes[len=" + (body == null ? 0 : body.length) +
            "]}";
    }
}
