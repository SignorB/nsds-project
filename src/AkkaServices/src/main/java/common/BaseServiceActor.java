package common;

import akka.actor.typed.Behavior;
import akka.actor.typed.PostStop;
import akka.actor.typed.PreRestart;
import akka.actor.typed.javadsl.*;
import common.kafka.KafkaService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for all Akka service actors in this project.
 * Handles logging, Kafka access, and actor lifecycle signals.
 * Child classes must implement {@link #getServiceName()} and {@link #addMessageHandlers(ReceiveBuilder)}.
 *
 * @param <C> command type this actor receives
 */
public abstract class BaseServiceActor<C> extends AbstractBehavior<C> {

    protected final Logger log;
    protected final KafkaService kafka;

    protected BaseServiceActor(ActorContext<C> context, KafkaService kafka) {
        super(context);
        this.kafka = kafka;
        this.log = LoggerFactory.getLogger(getClass());
        log.info("{} actor started", getServiceName());
    }

    /**
     * Wraps actor creation in the standard Akka {@code Behaviors.setup} block.
     *
     * @param <T>     command type
     * @param factory function that builds the actor given a context
     * @return the resulting {@link Behavior}
     */
    public static <T> Behavior<T> create(java.util.function.Function<ActorContext<T>, AbstractBehavior<T>> factory) {
        return Behaviors.setup(factory::apply);
    }

    protected abstract String getServiceName();

    protected abstract void addMessageHandlers(ReceiveBuilder<C> builder);

    /**
     * Builds the receive behavior by combining child-defined handlers with lifecycle signal handlers.
     */
    @Override
    public final Receive<C> createReceive() {
        ReceiveBuilder<C> builder = newReceiveBuilder();

        addMessageHandlers(builder);

        builder.onSignal(PreRestart.class, preRestart -> onPreRestart());
        builder.onSignal(PostStop.class, postStop -> onPostStop());

        return builder.build();
    }

    protected Behavior<C> onPreRestart() {
        log.warn("{} is restarting", getServiceName());
        return this;
    }

    protected Behavior<C> onPostStop() {
        log.info("{} stopped", getServiceName());
        return this;
    }

    /**
     * Sends an event to the Kafka topic identified by the given config key.
     *
     * @param topicConfigKey config path for the topic name
     * @param event          event to send
     */
    protected void sendEvent(String topicConfigKey, common.events.Event event) {
        kafka.sendEvent(topicConfigKey, event);
    }
}
