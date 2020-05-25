package de.tu_berlin.cit.iot_delivery_vehicles_experiment.producer;

import akka.actor.ActorRef;
import de.tu_berlin.cit.iot_delivery_vehicles_experiment.common.iot_delivery_vehicles.Point;
import de.tu_berlin.cit.iot_delivery_vehicles_experiment.common.iot_delivery_vehicles.TrafficEvent;
import de.tu_berlin.cit.iot_delivery_vehicles_experiment.producer.Vehicles.VehicleActor;
import de.tu_berlin.cit.iot_delivery_vehicles_experiment.producer.Vehicles.VehicleActor.Emit;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.log4j.Logger;

import java.time.LocalTime;
import java.time.ZoneId;
import java.util.List;
import java.util.concurrent.TimeUnit;

public enum Generator { GET;

    private static final Logger LOG = Logger.getLogger(Generator.class);

    private final StopWatch stopWatch = new StopWatch();

    // class for calculating traffic rate at time of day in seconds using sinusoidal function
    private static class TrafficRate {

        private int amplitude;
        private int period;
        private int verticalShift;

        TrafficRate(int amplitude, int period, int verticalShift) {
            this.amplitude = amplitude;
            this.period = period;
            this.verticalShift = verticalShift;
        }

        int getLimit() {
            long seconds = Generator.GET.stopWatch.getTime(TimeUnit.SECONDS);
            return (int) (- amplitude * Math.cos(((2 * Math.PI) / period) * seconds) + verticalShift);
        }
    }

    public void generate(
            String graphFileName, int updateInterval,
            int amplitude, int period, int verticalPhase,
            String topic, KafkaProducer<String, TrafficEvent> kafkaProducer) throws Exception {

        // import and generate street graph from file
        RoutesGraph.GET.importFromResource(graphFileName);
        TrafficRate rate = new TrafficRate(amplitude, period, verticalPhase);

        // start stopwatch
        LocalTime now = LocalTime.now(ZoneId.systemDefault());
        LOG.info("Traffic Generator Start Time: " + now.toString() + ", seconds of day: " + now.toSecondOfDay());
        Generator.GET.stopWatch.start();

        while (true) {
            if (VehicleActor.VEHICLE_COUNT.get() < rate.getLimit()) {
                List<Point> waypoints = RoutesGraph.GET.getRandomRoute();
                ActorRef vehicleActor = Vehicles.SYSTEM.actorOf(VehicleActor.props(updateInterval, waypoints));
                vehicleActor.tell(new Emit(topic, kafkaProducer), ActorRef.noSender());
            }
        }
    }
}
