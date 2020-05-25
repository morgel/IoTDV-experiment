package de.tu_berlin.cit.iot_delivery_vehicles_experiment.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.tu_berlin.cit.iot_delivery_vehicles_experiment.common.iot_delivery_vehicles.TrafficEvent;
import de.tu_berlin.cit.iot_delivery_vehicles_experiment.common.utils.Resources;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.log4j.Logger;

import java.util.Map;
import java.util.Properties;

public class Run {

    private static final Logger LOG = Logger.getLogger(Run.class);

    public static class TrafficEventSerializer implements Serializer<TrafficEvent> {

        private static final Logger LOG = Logger.getLogger(TrafficEventSerializer.class);

        private final ObjectMapper objectMap = new ObjectMapper();

        @Override
        public void configure(Map<String, ?> configs, boolean isKey) { }

        @Override
        public byte[] serialize(String topic, TrafficEvent trafficEvent) {
            try {
                String msg = objectMap.writeValueAsString(trafficEvent);
                return msg.getBytes();
            }
            catch(JsonProcessingException ex) {
                LOG.error("Error in Serialization", ex);
            }
            return null;
        }

        @Override
        public void close() { }
    }

    public static void main(String[] args) throws Exception {

        // ensure checkpoint interval is supplied as an argument
        if (args.length != 1) {
            throw new IllegalStateException("Required Command line argument: [TRAFFIC_GENERATOR.RATE.VERTICAL_PHASE]");
        }
        int verticalPhase = Integer.parseInt(args[0]);

        // get properties file
        Properties iotDataProducerProps = Resources.GET.read("iot_traffic.properties", Properties.class);

        Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers", iotDataProducerProps.getProperty("kafka.brokerList"));
        kafkaProps.put("acks", "all");
        kafkaProps.put("retries", 0);
        kafkaProps.put("batch.size", 16384);
        kafkaProps.put("linger.ms", 1);
        kafkaProps.put("buffer.memory", 33554432);
        kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("value.serializer", TrafficEventSerializer.class.getName());

        KafkaProducer<String, TrafficEvent> kafkaProducer = new KafkaProducer<>(kafkaProps);

        // initialise generation of vehicle events
        Generator.GET
            .generate(
                iotDataProducerProps.getProperty("trafficGenerator.graphFileName"),
                Integer.parseInt(iotDataProducerProps.getProperty("trafficGenerator.updateInterval")),
                Integer.parseInt(iotDataProducerProps.getProperty("trafficGenerator.rate.amplitude")),
                Integer.parseInt(iotDataProducerProps.getProperty("trafficGenerator.rate.period")),
                //Integer.parseInt(iotDataProducerProps.getProperty("trafficGenerator.rate.verticalPhase")),
                verticalPhase,
                iotDataProducerProps.getProperty("kafka.topic"),
                kafkaProducer);
    }
}
