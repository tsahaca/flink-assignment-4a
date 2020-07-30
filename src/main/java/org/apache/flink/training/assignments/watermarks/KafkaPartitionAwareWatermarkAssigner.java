package org.apache.flink.training.assignments.watermarks;


import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.training.assignments.domain.Order;


public class KafkaPartitionAwareWatermarkAssigner extends AscendingTimestampExtractor<Order> {

    @Override
    public long extractAscendingTimestamp(Order order) {
        //return order.getTimestamp();
        return System.currentTimeMillis() - 10;
    }
}
