package common;

import akka.actor.typed.ActorRef;
import akka.actor.typed.ActorSystem;
import akka.actor.typed.Behavior;
import akka.actor.typed.SupervisorStrategy;
import akka.actor.typed.javadsl.Behaviors;
import common.kafka.KafkaService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/**
 * Handles the complexity of starting an Akka service: creates the actor system,
 * sets up Kafka, creates the service actor with supervision, and starts all Kafka consumers.
 */
public class ServiceLauncher {

    private static final Logger log = LoggerFactory.getLogger(ServiceLauncher.class);

    /**
     * Convenience method to launch a service from a pre-built list of subscriptions.
     *
     * @param <C>           command type the actor receives
     * @param serviceName   name of the service (used for logging)
     * @param actorName     actor name used internally by Akka
     * @param actorFactory  function that creates the actor behavior given a {@link KafkaService}
     * @param subscriptions list of Kafka topic subscriptions
     */
    public static <C> void launch(
            String serviceName,
            String actorName,
            Function<KafkaService, Behavior<C>> actorFactory,
            List<TopicSubscription<C>> subscriptions) {

        Builder<C> builder = new Builder<C>()
                .serviceName(serviceName)
                .actorName(actorName)
                .actorFactory(actorFactory);

        for (TopicSubscription<C> sub : subscriptions) {
            builder.addSubscription(sub);
        }

        builder.launch();
    }

    /**
     * Builder for configuring and launching a service.
     */
    public static class Builder<C> {
        private final List<TopicSubscription<C>> subscriptions = new ArrayList<>();
        private String serviceName;
        private String actorName;
        private Function<KafkaService, Behavior<C>> actorFactory;
        private int maxRestarts = 5;
        private Duration restartWindow = Duration.ofMinutes(1);

        public Builder<C> serviceName(String serviceName) {
            this.serviceName = serviceName;
            return this;
        }

        public Builder<C> actorName(String actorName) {
            this.actorName = actorName;
            return this;
        }

        public Builder<C> actorFactory(Function<KafkaService, Behavior<C>> actorFactory) {
            this.actorFactory = actorFactory;
            return this;
        }

        public Builder<C> addSubscription(TopicSubscription<C> subscription) {
            this.subscriptions.add(subscription);
            return this;
        }

        /**
         * @param consumerConfig config path for consumer settings
         * @param topicConfigKey config path for the topic name
         * @param messageParser  converts a JSON string to a command
         */
        public Builder<C> addSubscription(
                String consumerConfig,
                String topicConfigKey,
                Function<String, C> messageParser) {
            return addSubscription(TopicSubscription.create(consumerConfig, topicConfigKey, messageParser));
        }

        public Builder<C> maxRestarts(int maxRestarts) {
            this.maxRestarts = maxRestarts;
            return this;
        }

        public Builder<C> restartWindow(Duration restartWindow) {
            this.restartWindow = restartWindow;
            return this;
        }

        /**
         * Starts the Akka actor system and all Kafka consumers.
         */
        public void launch() {
            if (serviceName == null || actorName == null || actorFactory == null) {
                throw new IllegalStateException("serviceName, actorName, and actorFactory must be set");
            }

            if (subscriptions.isEmpty()) {
                throw new IllegalStateException("At least one topic subscription must be added");
            }

            String systemName = serviceName.replace(" ", "") + "System";
            Behavior<Void> guardian = createGuardian();

            ActorSystem.create(guardian, systemName);
            log.info("{} application started", serviceName);
        }

        private Behavior<Void> createGuardian() {
            return Behaviors.setup(context -> {
                KafkaService kafkaService = new KafkaService(context.getSystem(), serviceName);

                // Spawn the service actor with a restart-on-failure supervision strategy
                ActorRef<C> serviceActor = context.spawn(
                        Behaviors.supervise(actorFactory.apply(kafkaService))
                                .onFailure(SupervisorStrategy.restart()
                                        .withLimit(maxRestarts, restartWindow)
                                        .withLoggingEnabled(true)),
                        actorName
                );

                log.info("{} actor started with supervision (max {} restarts in {})",
                        serviceName, maxRestarts, restartWindow);

                for (TopicSubscription<C> sub : subscriptions) {
                    KafkaService.startConsumer(
                            context.getSystem(),
                            serviceName,
                            sub.getConsumerConfig(),
                            sub.getTopicConfigKey(),
                            serviceActor,
                            sub.getMessageParser()
                    );
                }

                log.info("{} subscribed to {} topic(s)", serviceName, subscriptions.size());

                // Return empty behavior - guardian doesn't handle messages
                return Behaviors.empty();
            });
        }
    }
}
