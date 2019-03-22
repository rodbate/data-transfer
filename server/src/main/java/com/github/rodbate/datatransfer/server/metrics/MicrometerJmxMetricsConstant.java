package com.github.rodbate.datatransfer.server.metrics;


import lombok.extern.slf4j.Slf4j;

/**
 * User: rodbate
 * Date: 2018/12/19
 * Time: 15:58
 */
@Slf4j
public class MicrometerJmxMetricsConstant {

    /**
     * kafka message send batch metrics
     */
    private static final String KAFKA_MESSAGE_SEND_BATCH_METRIC_NAME_PREFIX = "kafkaMessageSendBatch";
    public static final String KAFKA_MESSAGE_SEND_BATCH_COST_TIME_METRIC_NAME = KAFKA_MESSAGE_SEND_BATCH_METRIC_NAME_PREFIX + ".costTime";
    public static final String KAFKA_MESSAGE_SEND_BATCH_INVOKED_SUCCESS_COUNT_METRIC_NAME = KAFKA_MESSAGE_SEND_BATCH_METRIC_NAME_PREFIX + ".invoked.successCount";
    public static final String KAFKA_MESSAGE_SEND_BATCH_INVOKED_FAILED_COUNT_METRIC_NAME = KAFKA_MESSAGE_SEND_BATCH_METRIC_NAME_PREFIX + ".invoked.failedCount";
    public static final String KAFKA_MESSAGE_SEND_BATCH_MESSAGE_SUCCESS_TOTAL_BYTES_METRIC_NAME = KAFKA_MESSAGE_SEND_BATCH_METRIC_NAME_PREFIX + ".message.success.totalBytes";
    public static final String KAFKA_MESSAGE_SEND_BATCH_MESSAGE_SUCCESS_TOTAL_COUNT_METRIC_NAME = KAFKA_MESSAGE_SEND_BATCH_METRIC_NAME_PREFIX + ".message.success.totalCount";
    public static final String KAFKA_MESSAGE_SEND_BATCH_MESSAGE_FAILED_TOTAL_BYTES_METRIC_NAME = KAFKA_MESSAGE_SEND_BATCH_METRIC_NAME_PREFIX + ".message.failed.totalBytes";
    public static final String KAFKA_MESSAGE_SEND_BATCH_MESSAGE_FAILED_TOTAL_COUNT_METRIC_NAME = KAFKA_MESSAGE_SEND_BATCH_METRIC_NAME_PREFIX + ".message.failed.totalCount";

    /**
     * deduplicate message service metrics
     */
    private static final String DEDUPLICATE_MESSAGE_SERVICE_METRIC_NAME_PREFIX = "deduplicateMessageService";
    public static final String DEDUPLICATE_MESSAGE_SERVICE_DUPLICATED_COUNT_METRIC_NAME = DEDUPLICATE_MESSAGE_SERVICE_METRIC_NAME_PREFIX + ".duplicatedCount";
    public static final String DEDUPLICATE_MESSAGE_SERVICE_DATA_PREV_LOWER_EIGHT_SIZE_METRIC_NAME = DEDUPLICATE_MESSAGE_SERVICE_METRIC_NAME_PREFIX + ".data.pre.lowerEightSize";
    public static final String DEDUPLICATE_MESSAGE_SERVICE_DATA_PREV_UPPER_EIGHT_SIZE_METRIC_NAME = DEDUPLICATE_MESSAGE_SERVICE_METRIC_NAME_PREFIX + ".data.pre.upperEightSize";
    public static final String DEDUPLICATE_MESSAGE_SERVICE_DATA_CURRENT_LOWER_EIGHT_SIZE_METRIC_NAME = DEDUPLICATE_MESSAGE_SERVICE_METRIC_NAME_PREFIX + ".data.current.lowerEightSize";
    public static final String DEDUPLICATE_MESSAGE_SERVICE_DATA_CURRENT_UPPER_EIGHT_SIZE_METRIC_NAME = DEDUPLICATE_MESSAGE_SERVICE_METRIC_NAME_PREFIX + ".data.current.upperEigthSize";

}
