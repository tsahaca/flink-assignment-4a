package org.apache.flink.training.assignments.keys;


import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.training.assignments.domain.Allocation;
import org.apache.flink.training.assignments.domain.Position;
import org.apache.flink.training.assignments.domain.PositionByCusip;

import java.util.ArrayList;
import java.util.List;

public class PositionAggregatorBySymbol
		implements AggregateFunction<Position, PositionByCusip, PositionByCusip> {

	/**
	 * 
	 */
	private static final long serialVersionUID = -8528772774907786176L;
	
	@Override
	public PositionByCusip createAccumulator() {
		PositionByCusip positionByCusip = new PositionByCusip();
		positionByCusip.setTimestamp(System.currentTimeMillis());
		return positionByCusip;
	}

	@Override
	public PositionByCusip add(Position value, PositionByCusip accumulator) {
		accumulator.setCusip(value.getCusip());
		accumulator.setQuantity(accumulator.getQuantity() + value.getQuantity());
		accumulator.setOrderId(value.getOrderId());
		return accumulator;
	}

	@Override
	public PositionByCusip getResult(PositionByCusip accumulator) {
		return accumulator;
	}

	@Override
	public PositionByCusip merge(PositionByCusip a,
								 PositionByCusip b) {

		PositionByCusip pcusip=new PositionByCusip(a.getCusip(),
				a.getQuantity()+b.getQuantity(), a.getOrderId());
		pcusip.setTimestamp(System.currentTimeMillis());


		return pcusip;
	}

}
