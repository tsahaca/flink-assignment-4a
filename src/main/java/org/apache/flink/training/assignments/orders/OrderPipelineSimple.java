package org.apache.flink.training.assignments.orders;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.training.assignments.domain.*;
import org.apache.flink.training.assignments.functions.OrderFlatMapCusip;
import org.apache.flink.training.assignments.functions.OrderMapCusip;
import org.apache.flink.training.assignments.keys.AccountPositionKeySelector;
import org.apache.flink.training.assignments.keys.OrderFlatMap;
import org.apache.flink.training.assignments.serializers.OrderKafkaDeserializationSchema;
import org.apache.flink.training.assignments.serializers.PositionKeyedSerializationSchema;
import org.apache.flink.training.assignments.serializers.SymbolKeyedSerializationSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;


/**
 * The main class to process block orders received
 * from kafka and create positions by account, sub-account
 * cusip and publish to kafka
 */
public class OrderPipelineSimple {
    private final String KAFKA_ADDRESS;
    private final String IN_TOPIC;
    private final String OUT_TOPIC;
    private final String KAFKA_GROUP;
    private final String OUT_CUSIP;
    private final int WINDOW_SIZE;

    private static final Logger LOG = LoggerFactory.getLogger(OrderPipelineSimple.class);

    public OrderPipelineSimple(final Map<String,Object> params){
        this.KAFKA_ADDRESS=(String)params.get(IConstants.KAFKA_ADDRESS);
        this.IN_TOPIC=(String)params.get(IConstants.IN_TOPIC);
        this.OUT_TOPIC=(String)params.get(IConstants.OUT_TOPIC);
        this.KAFKA_GROUP=(String)params.get(IConstants.KAFKA_GROUP);
        this.OUT_CUSIP=(String)params.get(IConstants.OUT_CUSIP);
        this.WINDOW_SIZE=(int) params.get(IConstants.WINDOW_SIZE);

    }

    public void execute() throws Exception{
        // set up streaming execution environment
        var env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.disableOperatorChaining();


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
         * Split the orders by allocations to create Positions By Act, Sub Act and Symbol
         */
        var splitOrderByAllocation = splitOrderStream(orderStream);

        var positionsByAct =  splitOrderByAllocation.keyBy(new AccountPositionKeySelector())
                .sum("quantity")
                .name("AggregatePositionByActSubActCusip")
                .uid("AggregatePositionByActSubActCusip");

        /**
         * Publish the positions to kafka
         * set account number as the key of Kafa Record
         */
        FlinkKafkaProducer010<Position> flinkKafkaProducer = new FlinkKafkaProducer010<Position>(
                KAFKA_ADDRESS, OUT_TOPIC, new PositionKeyedSerializationSchema(OUT_TOPIC));
        positionsByAct.addSink(flinkKafkaProducer)
                .name("PublishPositionByActToKafka")
                .uid("PublishPositionByActToKafka");

        /**
         * Aggegate Positions by Cusip and publish to kafka
         */

        var positionsByCusip = aggregatePositionsBySymbol(orderStream)
                .keyBy(positionByCusip -> positionByCusip.getCusip())
                .sum("quantity")
                .name("AggregatePositionBySymbol")
                .uid("AggregatePositionBySymbol");

        FlinkKafkaProducer010<PositionByCusip> flinkKafkaProducerCusip = new FlinkKafkaProducer010<PositionByCusip>(
                KAFKA_ADDRESS, OUT_CUSIP, new SymbolKeyedSerializationSchema(OUT_CUSIP));
        positionsByCusip.addSink(flinkKafkaProducerCusip)
                .name("PublishPositionByCusipToKafka")
                .uid("PublishPositionByCusipToKafka");


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

    private DataStream<PositionByCusip> aggregatePositionsBySymbol(DataStream<Order> orderStream) {
        var cusipPositions =  orderStream
                .map(new OrderMapCusip())
                .name("MapOrderToPositionByCusip")
                .uid("MapOrderToPositionByCusip");
        return cusipPositions;
    }



}
