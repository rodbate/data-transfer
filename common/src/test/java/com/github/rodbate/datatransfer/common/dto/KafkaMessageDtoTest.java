package com.github.rodbate.datatransfer.common.dto;

import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * User: rodbate
 * Date: 2018/12/21
 * Time: 16:06
 */
public class KafkaMessageDtoTest extends Assert {


    @Test
    public void testCodec() {
        List<KafkaMessageDto> data = new ArrayList<>();
        data.add(new KafkaMessageDto("topic-1", "message-test-1-2. xx".getBytes(StandardCharsets.UTF_8)));
        data.add(new KafkaMessageDto("topic-2", "message-test-2-1. xx".getBytes(StandardCharsets.UTF_8)));
        data.add(new KafkaMessageDto("topic-3", "message-test-3. MNN".getBytes(StandardCharsets.UTF_8)));
        data.add(new KafkaMessageDto("topic-1", "message-test-1. 中文测试".getBytes(StandardCharsets.UTF_8)));

        byte[] bytes = KafkaMessageDto.encode(data);
        List<KafkaMessageDto> decodedMessage = KafkaMessageDto.decode(bytes);
        assertTrue(Arrays.deepEquals(data.toArray(), decodedMessage.toArray()));
    }

}
