package org.apache.flink.training.assignments.orders;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.training.assignments.domain.Position;
import org.apache.flink.training.assignments.keys.AccountPositionKeySelector;
import org.apache.flink.training.assignments.keys.PositionAggregationWindowFunction;
import org.apache.flink.training.assignments.sinks.LogSink;
import org.apache.flink.training.assignments.utils.ExerciseBase;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

public class DataStreamTest extends ExerciseBase {
    private static final Logger LOG = LoggerFactory.getLogger(DataStreamTest.class);

    public static void main(String[] args) throws Exception{
        var env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        env.setParallelism(1);
        DataStream<Position> positionDataStream=
                env.fromCollection(Arrays.asList(
                        new Position(
                                "AC1",
                                "SB1",
                                "CUSIP1",
                                100,"Order-100"
                        ),
                        new Position(
                                "AC1",
                                "SB1",
                                "CUSIP1",
                                250,"Order-200"
                        ),
                        new Position(
                                "AC1",
                                "SB1",
                                "CUSIP1",
                                -70,"Order-300"
                        ),
                        new Position(
                                "AC2",
                                "SB2",
                                "CUSIP2",
                                300,"Order-400"
                        ),
                        new Position(
                                "AC2",
                                "SB2",
                                "CUSIP2",
                                -250,"Order-500"
                        ),
                        new Position(
                                "AC2",
                                "SB2",
                                "CUSIP2",
                                35,"Order-600"
                        )
                ));
        positionDataStream.addSink(new LogSink<>(LOG,
                LogSink.LoggerEnum.INFO, "**** inputStream {}"));

        var output = positionDataStream.keyBy(new AccountPositionKeySelector())
                .sum("quantity");

        output.addSink(new LogSink<>(LOG,
                LogSink.LoggerEnum.INFO, "**** positionsByActSubActCusip {}"));

        var cusip = positionDataStream.keyBy(position -> position.getCusip())
                .sum("quantity");
        //.apply(new PositionAggregationWindowFunction());
        cusip.addSink(new LogSink<>(LOG,
                LogSink.LoggerEnum.INFO, "**** positionsBySymbol {}"));




        env.execute("DataStreamTest");


    }
}
