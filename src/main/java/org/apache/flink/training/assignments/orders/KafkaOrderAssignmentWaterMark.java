package org.apache.flink.training.assignments.orders;


import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.training.assignments.domain.Allocation;
import org.apache.flink.training.assignments.domain.BuySell;
import org.apache.flink.training.assignments.domain.Position;
import org.apache.flink.training.assignments.domain.Order;
import org.apache.flink.training.assignments.keys.AccountPositionKeySelector;
import org.apache.flink.training.assignments.keys.PositionAggregationWindowFunction;
import org.apache.flink.training.assignments.serializers.*;
import org.apache.flink.training.assignments.sinks.LogSink;
import org.apache.flink.training.assignments.utils.ExerciseBase;
import org.apache.flink.training.assignments.watermarks.OrderPeriodicWatermarkAssigner;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;


public class KafkaOrderAssignmentWaterMark extends ExerciseBase {

    //public static final String KAFKA_ADDRESS = "kafka.dest.tanmay.wsn.riskfocus.com:9092";
    /**
    public static final String KAFKA_ADDRESS = "localhost:9092";
    public static final String IN_TOPIC = "in";
    public static final String OUT_TOPIC = "demo-output";
    */


    public static final String KAFKA_GROUP = "";
    private static final Logger LOG = LoggerFactory.getLogger(KafkaOrderAssignmentWaterMark.class);

    public static void main(String[] args) throws Exception {
        final String KAFKA_ADDRESS;
        final String IN_TOPIC;
        final String OUT_TOPIC;

        try {
            final ParameterTool params = ParameterTool.fromArgs(args);
            IN_TOPIC = params.has("IN_TOPIC") ? params.get("IN_TOPIC") : "in";
            OUT_TOPIC = params.has("OUT_TOPIC") ? params.get("OUT_TOPIC") : "demo-output";
            KAFKA_ADDRESS = params.getRequired("KAFKA_ADDRESS");
        } catch (Exception e) {
            System.err.println("No KAFKA_ADDRESS specified. Please run 'KafkaOrderAssignment \n" +
                    "--KAFKA_ADDRESS <localhost:9092> --IN_TOPIC <in> --OUT_TOPIC <demo-output>', \n" +
                    "where KAFKA_ADDRESS is bootstrap-server and \n" +
                    "IN_TOPIC is order input topic and \n" +
                    "OUT_TOPIC is position output topic");
            return;
        }

        // set up streaming execution environment
        var env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setAutoWatermarkInterval(10000);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(ExerciseBase.parallelism);

        // Extra credit: How would you use KafkaDeserializationSchema instead?

        Properties props = new Properties();
        props.setProperty("bootstrap.servers", KAFKA_ADDRESS);
        props.setProperty("group.id", KAFKA_GROUP);

        Properties prodProps = new Properties();
        prodProps.put("bootstrap.servers", KAFKA_ADDRESS);


        // Create tbe Kafka Consumer here
        // Added KafkaDeserializationSchema
        FlinkKafkaConsumer010<Order> flinkKafkaConsumer = new FlinkKafkaConsumer010(IN_TOPIC,
                new OrderKafkaDeserializationSchema(), props);

        //var orderStream = env.addSource(flinkKafkaConsumer.
        //        assignTimestampsAndWatermarks(new KafkaPartitionAwareWatermarkAssigner()));

        var orderStream = env.addSource(flinkKafkaConsumer);
        orderStream.addSink(new LogSink<>(LOG,
                LogSink.LoggerEnum.INFO, "**** orderStream {}"));

        /**
         * Splitting the orders by allocation
         */
        var splitOrderByAccountStream = orderStream
                //.assignTimestampsAndWatermarks(new KafkaPartitionAwareWatermarkAssigner())
                .assignTimestampsAndWatermarks(new OrderPeriodicWatermarkAssigner())
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(10)))
                .process(new ProcessAllWindowFunction<Order, Position, TimeWindow>() {
                    @Override
                    public void process(Context arg0,
                                        Iterable<Order> inputOrders,
                                        Collector<Position> outputOrders) throws Exception {
                        for (Order order : inputOrders) {
                            LOG.debug("Getting accounts for orderId= {}", order.getOrderId());
                            order.getAllocations().forEach(allocation -> {
                                outputOrders.collect(
                                        Position.builder()
                                        .account(allocation.getAccount())
                                        .subAccount(allocation.getSubAccount())
                                        .cusip(order.getCusip())
                                        .quantity(adjustQuantity(order,allocation))
                                        .build());
                            });
                        }
                    }
                });
        splitOrderByAccountStream.addSink(new LogSink<>(LOG,
                LogSink.LoggerEnum.INFO, "**** splitOrderByAccountStream {}"));

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


        aggregatedPositionsByAccountStream.addSink(new LogSink<>(LOG,
                LogSink.LoggerEnum.INFO, "**** aggregatedPositionsByAccountStream {}"));

        /**
         * Publish the positions by account,sub-account and cusip
         * set account number as the key of Kafa Record
         */
        FlinkKafkaProducer010<Position> flinkKafkaProducer = new FlinkKafkaProducer010<Position>(
                KAFKA_ADDRESS, OUT_TOPIC, new PositionKeyedSerializationSchema(OUT_TOPIC));
        aggregatedPositionsByAccountStream.addSink(flinkKafkaProducer);


        // execute the transformation pipeline
        env.execute("kafkaOrders");
    }

    private static int adjustQuantity(final Order order, final Allocation allocation){
        return order.getBuySell()== BuySell.BUY ? allocation.getQuantity() : -allocation.getQuantity();
    }

}
