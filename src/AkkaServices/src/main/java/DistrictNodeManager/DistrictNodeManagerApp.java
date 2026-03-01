package DistrictNodeManager;

import common.ServiceLauncher;
import common.TopicSubscription;
import common.events.NodeLifecycleEvent;

import java.util.List;
import java.util.UUID;

/**
 * Entry point for DistrictNodeManager. Subscribes to node requests, user lifecycle events
 * (for authorization), and its own output topic (for in-memory state rebuild)
 */
public class DistrictNodeManagerApp {

    public static void main() {
        if (System.getProperty("INSTANCE_ID") == null) {
            System.setProperty("INSTANCE_ID", UUID.randomUUID().toString());
        }

        List<TopicSubscription<DistrictNodeManager.Command>> subscriptions = List.of(

                TopicSubscription.create(
                        "kafka.node-consumer",
                        "kafka.topics.node-requests",
                        json -> new DistrictNodeManager.ProcessNodeRequest(NodeRequestEvent.fromJson(json))
                ),

                TopicSubscription.create(
                        "kafka.user-lifecycle-consumer",
                        "kafka.topics.user-lifecycle",
                        json -> new DistrictNodeManager.ProcessUserLifecycle(common.events.UserLifecycleEvent.fromJson(json))
                ),

                // also consume the output topic to keep in-memory node state consistent
                TopicSubscription.create(
                        "kafka.node-lifecycle-consumer",
                        "kafka.topics.node-lifecycle",
                        json -> new DistrictNodeManager.ProcessNodeLifecycle(NodeLifecycleEvent.fromJson(json))
                )
        );

        ServiceLauncher.launch(
                "DistrictNodeManager",
                "district-node-manager",
                DistrictNodeManager::create,
                subscriptions);
    }
}
