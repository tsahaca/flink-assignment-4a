package org.apache.flink.training.assignments.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.training.assignments.domain.BuySell;
import org.apache.flink.training.assignments.domain.Order;
import org.apache.flink.training.assignments.domain.Position;
import org.apache.flink.training.assignments.domain.PositionByCusip;

public class PositionMapCusip implements MapFunction<Position, PositionByCusip> {

    @Override
    public PositionByCusip map(Position value) throws Exception {

        PositionByCusip result = new PositionByCusip(value.getCusip(), value.getQuantity());
        result.setTimestamp(System.currentTimeMillis());
        return  result;
    }

}
