package org.apache.flink.training.assignments.serializers;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.training.assignments.domain.Order;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



public class OrderKafkaDeserializationSchema implements KafkaDeserializationSchema<Order>
{
    private static final Logger LOG = LoggerFactory.getLogger(OrderKafkaDeserializationSchema.class);

    static ObjectMapper objectMapper = new ObjectMapper();
            //.registerModule(new JavaTimeModule())
            //.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    /**
    static {
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }*/


    @Override
    public boolean isEndOfStream(Order nextElement) {
        return false;
    }

    @Override
    public Order deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
        LOG.debug("*** deserializing Kafka ConsumerRecord with key={}", record.key());
        Order order = objectMapper.readValue(record.value(), Order.class);
        order.setTimestamp(record.timestamp());
        LOG.debug("*** deserialized Kafka ConsumerRecord with key={}, orderId={}", record.key(), order.getOrderId());
        return order;
    }

    @Override
    public TypeInformation<Order> getProducedType() {
        return TypeInformation.of(Order.class);
    }
}