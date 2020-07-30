package org.apache.flink.training.assignments.orders;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.training.assignments.domain.Allocation;
import org.apache.flink.training.assignments.domain.BuySell;
import org.apache.flink.training.assignments.domain.Order;
import org.apache.flink.training.assignments.domain.Position;
import org.apache.flink.training.assignments.keys.AccountPositionKeySelector;
import org.apache.flink.training.assignments.keys.PositionAggregationWindowFunction;
import org.apache.flink.training.assignments.keys.SplitOrderWindowFunction;
import org.apache.flink.training.assignments.serializers.OrderKafkaDeserializationSchema;
import org.apache.flink.training.assignments.serializers.PositionKeyedSerializationSchema;
import org.apache.flink.training.assignments.sinks.LogSink;
import org.apache.flink.training.assignments.utils.ExerciseBase;
import org.apache.flink.training.assignments.watermarks.OrderPeriodicWatermarkAssigner;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;


/**
 * The main class to process block orders received
 * from kafka and create positions by account, sub-account
 * cusip and publish to kafka
 */
public class OrderPipeline {
    private final String KAFKA_ADDRESS;
    private final String IN_TOPIC;
    private final String OUT_TOPIC;
    private static final String KAFKA_GROUP = "";
    private static final Logger LOG = LoggerFactory.getLogger(OrderPipeline.class);

    public OrderPipeline(final String brokerUrl,
                         final String input_topic, final String output_topic){

        this.KAFKA_ADDRESS=brokerUrl;
        this.IN_TOPIC=input_topic;
        this.OUT_TOPIC=output_topic;

    }


    public void execute() throws Exception{
        // set up streaming execution environment
        var env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setAutoWatermarkInterval(10000);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //env.setParallelism(ExerciseBase.parallelism);

        /**
         * Create the Order Stream from Kafka
         */
        var orderStream = env.addSource(readFromKafka());
        orderStream.addSink(new LogSink<>(LOG,
                LogSink.LoggerEnum.INFO, "**** orderStream {}"));

        /**
         * Split the orders by allocations
         */
        var splitOrderByAccountStream = splitOrderStream(orderStream);
        splitOrderByAccountStream.addSink(new LogSink<>(LOG,
                LogSink.LoggerEnum.INFO, "**** splitOrderByAccountStream {}"));
        /**
         * Create positions by aggregating allocations
         * by account,sub-account and cusip
         */
        var aggregatedPositionsByAccountStream = createPositions(splitOrderByAccountStream);
        aggregatedPositionsByAccountStream.addSink(new LogSink<>(LOG,
                LogSink.LoggerEnum.INFO, "**** aggregatedPositionsByAccountStream {}"));

        /**
         * Publish the positions to kafka
         * set account number as the key of Kafa Record
         */
        FlinkKafkaProducer010<Position> flinkKafkaProducer = new FlinkKafkaProducer010<Position>(
                KAFKA_ADDRESS, OUT_TOPIC, new PositionKeyedSerializationSchema(OUT_TOPIC));
        aggregatedPositionsByAccountStream.addSink(flinkKafkaProducer);


        // execute the transformation pipeline
        env.execute("kafkaOrders");
    }

    /**
     * Read Block Orders from Kafka
     * @return
     */
    private FlinkKafkaConsumer010<Order> readFromKafka(){
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", KAFKA_ADDRESS);
        props.setProperty("group.id", KAFKA_GROUP);

        Properties prodProps = new Properties();
        prodProps.put("bootstrap.servers", KAFKA_ADDRESS);


        // Create tbe Kafka Consumer here
        // Added KafkaDeserializationSchema
        FlinkKafkaConsumer010<Order> flinkKafkaConsumer = new FlinkKafkaConsumer010(IN_TOPIC,
                new OrderKafkaDeserializationSchema(), props);
        return flinkKafkaConsumer;
    }

    /**
     * Split Orders by Account, sub-account and cusip
     */
    private DataStream<Position> splitOrderStream(final DataStream<Order> orderStream) {
        DataStream<Position> splitOrderByAccountStream = orderStream
                .assignTimestampsAndWatermarks(new OrderPeriodicWatermarkAssigner())
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(10)))
                .process(new SplitOrderWindowFunction());
        return splitOrderByAccountStream;
    }

    /**
     * Create positions
     * @param splitOrderByAccountStream
     * @return
     */
    private DataStream<Position> createPositions(final DataStream<Position> splitOrderByAccountStream){
        /**
         * Group the order by account, sub-account and cusip
         */
        var groupOrderByAccountWindowedStream=splitOrderByAccountStream
                .keyBy(new AccountPositionKeySelector())
                .timeWindow(Time.seconds(10));
        /**
         * Aggregate the position by account,sub-account and cusip
         */
        var aggregatedPositionsByAccountStream = groupOrderByAccountWindowedStream
                .apply(new PositionAggregationWindowFunction())
                .name("Aggregate Position Count in a Windowed stream");

        return aggregatedPositionsByAccountStream;
    }

}
