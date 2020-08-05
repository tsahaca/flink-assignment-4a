package org.apache.flink.training.assignments.orders;

public interface IConstants {
    String KAFKA_ADDRESS="KAFKA_ADDRESS";
    String DEFASULT_KAFKA_ADDRESS="--KAFKA_ADDRESS kafka.dest.tanmay.wsn.riskfocus.com:9092";
    String IN_TOPIC="IN_TOPIC";
    String OUT_TOPIC="OUT_TOPIC";
    String KAFKA_GROUP="KAFKA_GROUP";
    String OUT_CUSIP="OUT_CUSIP";
    String DEFAULT_OUT_TOPIC="positionsByAct";
    String DEFAULT_OUT_CUSIP="positionsBySymbol";
    String DEFAULT_IN_TOPIC="in";
 }
