package org.apache.flink.training.assignments.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.training.assignments.domain.Allocation;
import org.apache.flink.training.assignments.domain.BuySell;
import org.apache.flink.training.assignments.domain.Order;
import org.apache.flink.training.assignments.domain.PositionByCusip;
import org.apache.flink.util.Collector;

public class OrderMapCusip implements MapFunction<Order, PositionByCusip> {

    @Override
    public PositionByCusip map(Order value) throws Exception {

        PositionByCusip result = new PositionByCusip(value.getCusip(), adjustQuantity(value));
        result.setTimestamp(System.currentTimeMillis());
        return  result;
    }

    private  int adjustQuantity(final Order order){
        return order.getBuySell()== BuySell.BUY ? order.getQuantity() : -order.getQuantity();
    }
}
