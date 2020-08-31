package org.apache.flink.training.assignments.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.training.assignments.domain.*;
import org.apache.flink.util.Collector;

public class OrderFlatMapCusip implements FlatMapFunction<Order, PositionByCusip> {
    @Override
    public void flatMap(Order order, Collector<PositionByCusip> collector) throws Exception {
        PositionByCusip result = new PositionByCusip(order.getCusip(), adjustQuantity(order));
        result.setTimestamp(System.currentTimeMillis());
        collector.collect(result);
    }

    private  int adjustQuantity(final Order order){
        return order.getBuySell()== BuySell.BUY ? order.getQuantity() : -order.getQuantity();
    }
    /**
     *  private  int adjustQuantity(final Order order, final Allocation allocation){
     *         return order.getBuySell()== BuySell.BUY ? allocation.getQuantity() : -allocation.getQuantity();
     *     }
     */
}
