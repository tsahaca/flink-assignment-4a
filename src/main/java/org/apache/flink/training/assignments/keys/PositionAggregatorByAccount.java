package org.apache.flink.training.assignments.keys;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.training.assignments.domain.Position;

public class PositionAggregatorByAccount
		implements AggregateFunction<Position, Position, Position> {

	/**
	 * 
	 */
	private static final long serialVersionUID = -8528772774907786176L;
	
	@Override
	public Position createAccumulator() {
		return Position.builder().build();

	}

	@Override
	public Position add(Position value, Position accumulator) {
		accumulator.setQuantity(accumulator.getQuantity()+ value.getQuantity());
		return accumulator;
	}

	@Override
	public Position getResult(Position accumulator) {
		return accumulator;
	}

	@Override
	public Position merge(Position a, Position b) {
		return Position.builder()
				.account(a.getAccount())
				.subAccount(a.getSubAccount())
				.cusip(a.getCusip())
				.quantity(a.getQuantity()+b.getQuantity())
				.build();
	}

}
