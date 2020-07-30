package org.apache.flink.training.assignments.serializers;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;
import org.apache.flink.training.assignments.domain.Position;

public class PositionKeyedSerializationSchema implements KeyedSerializationSchema<Position> {
    static ObjectMapper objectMapper = new ObjectMapper();//.registerModule(new JavaTimeModule());

    private String topic;

    public PositionKeyedSerializationSchema(final String topic){
        this.topic=topic;
    }

    /**
     * set account number as the key of Kafa Record
     * @param element
     * @return
     */
    @Override
    public byte[] serializeKey(Position element) {
        return element.getAccount().getBytes();
    }

    @Override
    public byte[] serializeValue(Position element) {
        try {
            return objectMapper.writeValueAsBytes(element);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public String getTargetTopic(Position element) {
        return this.topic;
    }
}
