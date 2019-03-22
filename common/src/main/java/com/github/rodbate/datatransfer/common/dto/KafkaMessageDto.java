package com.github.rodbate.datatransfer.common.dto;

import lombok.Getter;
import lombok.Setter;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * User: rodbate
 * Date: 2018/12/18
 * Time: 12:06
 */
@Getter
@Setter
public class KafkaMessageDto {
    private final String topic;
    private final byte[] msg;

    public KafkaMessageDto(String topic, byte[] msg) {
        this.topic = Objects.requireNonNull(topic, "topic");
        this.msg = Objects.requireNonNull(msg, "msg");
    }

    public static byte[] encode(final List<KafkaMessageDto> messageList) {
        if (messageList == null || messageList.size() == 0) {
            return new byte[0];
        }

        int totalLength = 0;
        for (KafkaMessageDto msg : messageList) {
            totalLength += msg.encodeLength();
        }

        ByteBuffer buf = ByteBuffer.allocate(totalLength);
        for (KafkaMessageDto msg : messageList) {
            buf.put(msg.encode());
        }

        return buf.array();
    }

    public static List<KafkaMessageDto> decode(final byte[] data) {
        Objects.requireNonNull(data, "data");
        if (data.length == 0) {
            return Collections.emptyList();
        }
        final List<KafkaMessageDto> list = new LinkedList<>();
        final ByteBuffer buf = ByteBuffer.wrap(data);
        while (buf.hasRemaining()) {
            list.add(decode(buf));
        }
        return list;
    }

    private static KafkaMessageDto decode(final ByteBuffer buffer) {
        Objects.requireNonNull(buffer, "buffer");
        //1. topic
        short topicLen = buffer.getShort();
        byte[] topicBytes = new byte[topicLen];
        buffer.get(topicBytes);

        //2. msg body
        int msgLen = buffer.getInt();
        byte[] msgBytes = new byte[msgLen];
        buffer.get(msgBytes);
        return new KafkaMessageDto(new String(topicBytes, StandardCharsets.UTF_8), msgBytes);
    }

    public byte[] encode() {
        int totalLength = encodeLength();
        final ByteBuffer buf = ByteBuffer.allocate(totalLength);
        byte[] topicData = topic.getBytes(StandardCharsets.UTF_8);
        int topicLength = topicData.length;
        buf.putShort((short) topicLength);
        buf.put(topicData);
        buf.putInt(msg.length);
        buf.put(msg);
        return buf.array();
    }


    public int encodeLength() {
        int totalLength = 0;
        //1. short for topic length 2bytes
        totalLength += 2;
        //2. topic length
        byte[] topicData = topic.getBytes(StandardCharsets.UTF_8);
        int topicLength = topicData.length;
        if (topicLength > Short.MAX_VALUE) {
            throw new IllegalArgumentException(String.format("topic [name=%s, len=%d] length cannot exceed %d", topic, topicLength, Short.MAX_VALUE));
        }
        totalLength += topicLength;
        //3. msg len 4bytes
        totalLength += 4;
        //4. msg body length
        totalLength += msg.length;
        return totalLength;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        KafkaMessageDto that = (KafkaMessageDto) o;
        return topic.equals(that.topic) &&
            Arrays.equals(msg, that.msg);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(topic);
        result = 31 * result + Arrays.hashCode(msg);
        return result;
    }

    @Override
    public String toString() {
        return "KafkaMessageDto{" +
            "topic='" + topic + '\'' +
            ", msg=" + new String(msg, StandardCharsets.UTF_8) +
            '}';
    }
}
