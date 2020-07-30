package org.apache.flink.training.assignments.keys;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.training.assignments.domain.Position;
import org.apache.flink.util.Collector;

public class PositionAggregationWindowFunction implements WindowFunction<Position, Position,
        Tuple3<String, String, String>, TimeWindow> {

    @Override
    public void apply(
            final Tuple3<String, String, String> tuple,
            final TimeWindow timeWindow,
            final Iterable<Position> positions,
            final Collector<Position> collector
    ) throws Exception {

        //The main counting bit for position quantity
        int count = 0;
        for (Position position : positions
        ) {
            count += position.getQuantity();
        }

        Position aggregatedPosition = Position.builder()
                .account(tuple.f0)
                .subAccount(tuple.f1)
                .cusip(tuple.f2)
                .quantity(count)
                .build();
        collector.collect(aggregatedPosition);
    }
}
