package PingPongService;

import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.ReceiveBuilder;
import common.BaseServiceActor;
import common.kafka.KafkaService;
import org.jetbrains.annotations.NotNull;

/**
 * Simple test service that replies to "ping" messages on the ping-pong topic with a "pong" event.
 */
public class PingPongService extends BaseServiceActor<PingPongService.Command> {

    private static final String PING_PONG_TOPIC = "kafka.topics.ping-pong";

    public interface Command {
    }

    public record ProcessMessage(String message) implements Command {
    }

    private PingPongService(ActorContext<Command> context, KafkaService kafka) {
        super(context, kafka);
    }

    public static Behavior<Command> create(KafkaService kafka) {
        return BaseServiceActor.create(context -> new PingPongService(context, kafka));
    }

    @Override
    protected String getServiceName() {
        return "PingPongService";
    }

    @Override
    protected void addMessageHandlers(ReceiveBuilder<Command> builder) {
        builder.onMessage(ProcessMessage.class, this::onProcessMessage);
    }

    private Behavior<Command> onProcessMessage(@NotNull ProcessMessage command) {
        String message = command.message().trim();

        log.info("Received message: {}", message);

        if ("ping".equalsIgnoreCase(message)) {
            sendEvent(PING_PONG_TOPIC, PongEvent.createPong(message));
            log.info("Sent response: pong");
        } else {
            log.info("Ignoring non-ping message: {}", message);
        }

        return this;
    }
}
