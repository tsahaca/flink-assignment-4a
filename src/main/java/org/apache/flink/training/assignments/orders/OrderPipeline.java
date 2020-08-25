package org.apache.flink.training.assignments.orders;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.training.assignments.domain.Allocation;
import org.apache.flink.training.assignments.domain.Order;
import org.apache.flink.training.assignments.domain.Position;
import org.apache.flink.training.assignments.domain.PositionByCusip;
import org.apache.flink.training.assignments.keys.*;
import org.apache.flink.training.assignments.serializers.CusipKeyedSerializationSchema;
import org.apache.flink.training.assignments.serializers.OrderKafkaDeserializationSchema;
import org.apache.flink.training.assignments.serializers.PositionKeyedSerializationSchema;
import org.apache.flink.training.assignments.serializers.SymbolKeyedSerializationSchema;
import org.apache.flink.training.assignments.watermarks.OrderPeriodicWatermarkAssigner;
import org.apache.flink.training.assignments.watermarks.OrderWatermarkAssigner;
import org.apache.flink.training.assignments.watermarks.PositionPeriodicWatermarkAssigner;
import org.apache.flink.training.assignments.watermarks.PositionWatermarkAssigner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
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
    private final String KAFKA_GROUP;
    private final String OUT_CUSIP;
    private final int WM_INTERVAL;
    private final int WINDOW_SIZE;
    private final int OUT_ORDERNESS;


    private static final Logger LOG = LoggerFactory.getLogger(OrderPipeline.class);

    public OrderPipeline(final Map<String,Object> params){
        this.KAFKA_ADDRESS=(String)params.get(IConstants.KAFKA_ADDRESS);
        this.IN_TOPIC=(String)params.get(IConstants.IN_TOPIC);
        this.OUT_TOPIC=(String)params.get(IConstants.OUT_TOPIC);
        this.KAFKA_GROUP=(String) params.get(IConstants.KAFKA_GROUP);
        this.OUT_CUSIP=(String) params.get(IConstants.OUT_CUSIP);
        this.WM_INTERVAL=(int) params.get(IConstants.WM_INTERVAL);
        this.WINDOW_SIZE=(int) params.get(IConstants.WINDOW_SIZE);
        this.OUT_ORDERNESS=(int) params.get(IConstants.OUT_ORDERNESS);
    }


    public void execute() throws Exception{
        // set up streaming execution environment
        var env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setAutoWatermarkInterval(this.WM_INTERVAL);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.disableOperatorChaining();
        //env.setParallelism(ExerciseBase.parallelism);

        /**
         * Create the Order Stream from Kafka and keyBy cusip
         */
        var orderStream = env.addSource(readFromKafka())
                .name("kfkaTopicReader").uid("kfkaTopicReader")
                .assignTimestampsAndWatermarks(new OrderPeriodicWatermarkAssigner(this.OUT_ORDERNESS))
                .name("TimestampWatermark").uid("TimestampWatermark")
                //.rebalance()
                .keyBy(order -> order.getCusip());

        /**
        orderStream.addSink(new LogSink<>(LOG,
                LogSink.LoggerEnum.INFO, "**** orderStream {}"));
         */

        /**
         * Split the orders by allocations
         */
        var splitOrderByAccount = splitOrderStream(orderStream);
        /**
        splitOrderByAccount.addSink(new LogSink<>(LOG,
                LogSink.LoggerEnum.INFO, "**** splitOrderByAccount {}"));
         */
        /**
         * Create positions by aggregating allocations
         * by account,sub-account and cusip
         */
        var aggregatedPositionsByAccount = createPositions(splitOrderByAccount);
        //aggregatedPositionsByAccount.addSink(new LogSink<>(LOG,
                //LogSink.LoggerEnum.INFO, "**** aggregatedPositionsByAccount {}"));

        /**
         * Publish the positions to kafka
         * set account number as the key of Kafa Record
         */
        FlinkKafkaProducer010<Position> flinkKafkaProducer = new FlinkKafkaProducer010<Position>(
                KAFKA_ADDRESS, OUT_TOPIC, new PositionKeyedSerializationSchema(OUT_TOPIC));
        aggregatedPositionsByAccount.addSink(flinkKafkaProducer)
                .name("PublishPositionToKafka")
                .uid("PublishPositionToKafka");
        /**
         * Aggegate Positions by Cusip and publish to kafka
         */

        //var positionsByCusip = aggregatePositionsBySymbol(aggregatedPositionsByAccount);
        var positionsByCusip = aggregatePositionsBySymbol(splitOrderByAccount);
        FlinkKafkaProducer010<PositionByCusip> flinkKafkaProducerCusip = new FlinkKafkaProducer010<PositionByCusip>(
                KAFKA_ADDRESS, OUT_CUSIP, new SymbolKeyedSerializationSchema(OUT_CUSIP));
        positionsByCusip.addSink(flinkKafkaProducerCusip)
                .name("PublishPositionByCusipToKafka")
                .uid("PublishPositionByCusipToKafka");


        // execute the transformation pipeline
        System.out.println("Execution Plan");
        System.out.println(env.getExecutionPlan());
        env.execute("kafkaOrders");
    }

    /**
     * Read Block Orders from Kafka
     * @return
     */
    public FlinkKafkaConsumer010<Order> readFromKafka(){
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", KAFKA_ADDRESS);
        props.setProperty("group.id", KAFKA_GROUP);

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
                .flatMap(new OrderFlatMap())
                .name("splitOrderByAllocation")
                .uid("splitOrderByAllocation");

        /**
        .assignTimestampsAndWatermarks(new PositionPeriodicWatermarkAssigner(this.OUT_ORDERNESS))
        .name("TimestampWatermark").uid("TimestampWatermark");
         */
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
                //.timeWindow(Time.seconds(10))
                .window(TumblingEventTimeWindows.of(Time.seconds(this.WINDOW_SIZE)))
                .sum("quantity")
                .name("AggregatePositionByActSubActCusip")
                .uid("AggregatePositionByActSubActCusip");
        return groupOrderByAccountWindowedStream;
    }

    private DataStream<PositionByCusip> aggregatePositionsBySymbol(DataStream<Position> aggregatedPositionsByAccount){
        var positionsByCusip = aggregatedPositionsByAccount
                .keyBy(position -> position.getCusip())
                .window(TumblingEventTimeWindows.of(Time.seconds(this.WINDOW_SIZE)))

                //.timeWindow(Time.seconds(10))
                //.apply(new PositionByCusipWindowFunction())
                .aggregate(new PositionAggregatorBySymbol())
                .name("AggregatePositionBySymbol")
                .uid("AggregatePositionBySymbol");
        return positionsByCusip;
    }

}
