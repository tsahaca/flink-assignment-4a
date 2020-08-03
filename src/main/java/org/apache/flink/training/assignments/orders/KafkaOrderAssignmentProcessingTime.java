package org.apache.flink.training.assignments.orders;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.training.assignments.utils.ExerciseBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;


public class KafkaOrderAssignmentProcessingTime extends ExerciseBase {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaOrderAssignmentProcessingTime.class);

    /**
    // --KAFKA_ADDRESS kafka.dest.tanmay.wsn.riskfocus.com:9092 --IN_TOPIC in --OUT_TOPIC positionsByAct --OUT_CUSIP positionsBySymbol
    */
    public static void main(String[] args) throws Exception {

        final String KAFKA_ADDRESS;
        final String IN_TOPIC;
        final String OUT_TOPIC;
        final String KAFKA_GROUP;
        final String OUT_CUSIP; // positionsBySymbol

        try {
            final ParameterTool params = ParameterTool.fromArgs(args);
            IN_TOPIC = params.has("IN_TOPIC") ? params.get("IN_TOPIC") : IConstants.DEFAULT_IN_TOPIC;
            OUT_TOPIC = params.has("OUT_TOPIC") ? params.get("OUT_TOPIC") : IConstants.DEFAULT_OUT_TOPIC;
            KAFKA_ADDRESS = params.getRequired("KAFKA_ADDRESS");
            KAFKA_GROUP = params.has("KAFKA_GROUP") ? params.get("KAFKA_GROUP") : "";
            OUT_CUSIP = params.has("OUT_CUSIP") ? params.get("OUT_CUSIP") : IConstants.DEFAULT_OUT_CUSIP;
        } catch (Exception e) {
            System.err.println("No KAFKA_ADDRESS specified. Please run 'KafkaOrderAssignment \n" +
                    "--KAFKA_ADDRESS <localhost:9092> --IN_TOPIC <in> --OUT_TOPIC <demo-output>', \n" +
                    "where KAFKA_ADDRESS is bootstrap-server and \n" +
                    "IN_TOPIC is order input topic and \n" +
                    "OUT_TOPIC is position output topic");
            return;
        }
        final Map<String,String> params = new HashMap<String, String>();
        params.put(IConstants.KAFKA_ADDRESS, KAFKA_ADDRESS);
        params.put(IConstants.IN_TOPIC, IN_TOPIC);
        params.put(IConstants.OUT_TOPIC, OUT_TOPIC);
        params.put(IConstants.KAFKA_GROUP, KAFKA_GROUP);
        params.put(IConstants.OUT_CUSIP, OUT_CUSIP);

        final OrderPipelineProcessingTime pipeline = new OrderPipelineProcessingTime(params);
        pipeline.execute();
    }

}
