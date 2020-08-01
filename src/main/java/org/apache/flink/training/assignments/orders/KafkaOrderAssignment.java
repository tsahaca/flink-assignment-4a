package org.apache.flink.training.assignments.orders;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.training.assignments.utils.ExerciseBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



public class KafkaOrderAssignment extends ExerciseBase {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaOrderAssignment.class);



    public static void main(String[] args) throws Exception {

        final String KAFKA_ADDRESS;
        final String IN_TOPIC;
        final String OUT_TOPIC;
        final String KAFKA_GROUP;

        try {
            final ParameterTool params = ParameterTool.fromArgs(args);
            IN_TOPIC = params.has("IN_TOPIC") ? params.get("IN_TOPIC") : "in";
            OUT_TOPIC = params.has("OUT_TOPIC") ? params.get("OUT_TOPIC") : "demo-output";
            KAFKA_ADDRESS = params.getRequired("KAFKA_ADDRESS");
            KAFKA_GROUP = params.has("KAFKA_GROUP") ? params.get("KAFKA_GROUP") : "";
        } catch (Exception e) {
            System.err.println("No KAFKA_ADDRESS specified. Please run 'KafkaOrderAssignment \n" +
                    "--KAFKA_ADDRESS <localhost:9092> --IN_TOPIC <in> --OUT_TOPIC <demo-output>', \n" +
                    "where KAFKA_ADDRESS is bootstrap-server and \n" +
                    "IN_TOPIC is order input topic and \n" +
                    "OUT_TOPIC is position output topic");
            return;
        }

        final OrderPipeline pipeline = new OrderPipeline(KAFKA_ADDRESS,
                IN_TOPIC, OUT_TOPIC, KAFKA_GROUP);
        pipeline.execute();
    }

}
