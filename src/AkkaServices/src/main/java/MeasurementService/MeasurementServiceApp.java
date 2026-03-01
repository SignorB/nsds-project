package MeasurementService;

import common.ServiceLauncher;
import common.TopicSubscription;
import common.events.NodeLifecycleEvent;

import java.util.List;
import java.util.UUID;

/**
 * Entry point for MeasurementService. Subscribes to raw measurements and node lifecycle events.
 */
public class MeasurementServiceApp {

    public static void main() {
        if (System.getProperty("INSTANCE_ID") == null) {
            System.setProperty("INSTANCE_ID", UUID.randomUUID().toString());
        }

        List<TopicSubscription<MeasurementService.Command>> subscriptions = List.of(

                TopicSubscription.create(
                        "kafka.measurement-consumer",
                        "kafka.topics.measurements-raw",
                        json -> new MeasurementService.ProcessRawMeasurement(MeasurementRawEvent.fromJson(json))
                ),

                // node lifecycle events are needed to enrich/validate incoming measurements
                TopicSubscription.create(
                        "kafka.node-lifecycle-consumer",
                        "kafka.topics.node-lifecycle",
                        json -> new MeasurementService.ProcessNodeLifecycle(NodeLifecycleEvent.fromJson(json))
                )
        );

        ServiceLauncher.launch(
                "MeasurementService",
                "measurement-service",
                MeasurementService::create,
                subscriptions
        );
    }
}
