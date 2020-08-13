package org.apache.flink.training.assignments.watermarks;

import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.training.assignments.domain.Order;
import org.apache.flink.training.assignments.domain.Position;


/**
 * This generator generates watermarks assuming that elements arrive out of order,
 * but only to a certain degree. The latest elements for a certain timestamp t will arrive
 * at most n milliseconds after the earliest elements for timestamp t.
 */
public class PositionPeriodicWatermarkAssigner implements AssignerWithPeriodicWatermarks<Position> {
    private final long maxOutOfOrderness;// = 3500; // 3.5 seconds
    //private long lastwaterMark;
    private long currentMaxTimestamp;

    public PositionPeriodicWatermarkAssigner(final int outOfOrderness){
        this.maxOutOfOrderness=outOfOrderness;
    }


    @Override
    public long extractTimestamp(Position element, long previousElementTimestamp) {
        long timestamp = element.getTimestamp() == 0 ? System.currentTimeMillis() : element.getTimestamp();
        currentMaxTimestamp = Math.max(currentMaxTimestamp, timestamp);
        return timestamp;
    }

    @Override
    public Watermark getCurrentWatermark() {
        return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
    }
}