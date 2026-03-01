package PingPongService;

import common.ServiceLauncher;
import common.TopicSubscription;

import java.util.List;

/**
 * Entry point for PingPongService.
 */
public class PingPongServiceApp {

    public static void main() {
        List<TopicSubscription<PingPongService.Command>> subscriptions = List.of(
                TopicSubscription.create(
                        "kafka.ping-pong-consumer",
                        "kafka.topics.ping-pong",
                        message -> new PingPongService.ProcessMessage(message)
                )
        );

        ServiceLauncher.launch(
                "PingPongService",
                "ping-pong-service",
                PingPongService::create,
                subscriptions
        );
    }
}
