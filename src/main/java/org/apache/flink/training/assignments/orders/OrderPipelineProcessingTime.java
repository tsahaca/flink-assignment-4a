package org.apache.flink.training.assignments.orders;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.training.assignments.domain.Order;
import org.apache.flink.training.assignments.domain.Position;
import org.apache.flink.training.assignments.keys.AccountPositionKeySelector;
import org.apache.flink.training.assignments.keys.OrderFlatMap;

import org.apache.flink.training.assignments.serializers.OrderKafkaDeserializationSchema;
import org.apache.flink.training.assignments.serializers.PositionKeyedSerializationSchema;

import org.apache.flink.training.assignments.utils.ExerciseBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;


/**
 * The main class to process block orders received
 * from kafka and create positions by account, sub-account
 * cusip and publish to kafka
 */
public class OrderPipelineProcessingTime {
    private final String KAFKA_ADDRESS;
    private final String IN_TOPIC;
    private final String OUT_TOPIC;
    private final String KAFKA_GROUP;
    private final String OUT_CUSIP;
    private static final Logger LOG = LoggerFactory.getLogger(OrderPipelineProcessingTime.class);

    public OrderPipelineProcessingTime(final Map<String,String> params){
        this.KAFKA_ADDRESS=params.get(IConstants.KAFKA_ADDRESS);
        this.IN_TOPIC=params.get(IConstants.IN_TOPIC);
        this.OUT_TOPIC=params.get(IConstants.OUT_TOPIC);
        this.KAFKA_GROUP=params.get(IConstants.KAFKA_GROUP);
        this.OUT_CUSIP=params.get(IConstants.OUT_CUSIP);
    }

    public void execute() throws Exception{
        // set up streaming execution environment
        var env = StreamExecutionEnvironment.getExecutionEnvironment();
        //env.getConfig().setAutoWatermarkInterval(10000);
        //env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        env.setParallelism(ExerciseBase.parallelism);

        /**
         * Create the Order Stream from Kafka and keyBy cusip
         */
        var orderStream = env.addSource(readFromKafka())
                .name("kfkaTopicReader").uid("kfkaTopicReader")
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
                .uid("PublishPositionToKafka");;


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
         /**
        Properties prodProps = new Properties();
        prodProps.put("bootstrap.servers", KAFKA_ADDRESS);
        */


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
                .timeWindow(Time.seconds(10))
                //.window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .sum("quantity");

        return groupOrderByAccountWindowedStream;
    }



}
