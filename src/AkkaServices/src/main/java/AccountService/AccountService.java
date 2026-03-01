package AccountService;

import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.ReceiveBuilder;
import common.BaseServiceActor;
import common.events.UserLifecycleEvent;
import common.kafka.KafkaService;
import org.jetbrains.annotations.NotNull;

import java.util.HashMap;
import java.util.Map;

/**
 * Manages user accounts. Processes create/update/remove requests received via Kafka
 * and publishes the resulting lifecycle events back to Kafka.
 * In-memory state is kept consistent by also consuming the output lifecycle topic.
 */
public class AccountService extends BaseServiceActor<AccountService.Command> {

    private static final String OUTPUT_TOPIC = "kafka.topics.user-lifecycle";

    // in-memory user store, keyed by userId
    private final Map<String, User> users = new HashMap<>();

    private AccountService(ActorContext<Command> context, KafkaService kafka) {
        super(context, kafka);
    }

    /**
     * Factory method used by {@link common.ServiceLauncher} to create this actor.
     */
    public static Behavior<Command> create(KafkaService kafka) {
        return BaseServiceActor.create(context -> new AccountService(context, kafka));
    }

    @Override
    protected String getServiceName() {
        return "AccountService";
    }

    @Override
    protected void addMessageHandlers(ReceiveBuilder<Command> builder) {
        builder.onMessage(ProcessUserRequest.class, this::onProcessUserRequest);
        builder.onMessage(ProcessUserLifecycle.class, this::onProcessUserLifecycle);
    }

    private Behavior<Command> onProcessUserRequest(@NotNull ProcessUserRequest command) {
        UserRequestEvent event = command.event;
        String eventType = event.eventType();

        log.info("Processing request: type={}, eventId={}", eventType, event.eventId());

        switch (eventType) {
            case "UserRegisterRequest" -> handleRegister(event);
            case "UserUpdateRequest" -> handleUpdate(event);
            case "UserRemoveRequest" -> handleRemove(event);
            default -> {
                log.warn("Unknown request type: {}", eventType);
                sendRejection(event, "Unknown request type");
            }
        }

        return this;
    }

    /**
     * Keeps the in-memory user map consistent by consuming lifecycle events.
     */
    private Behavior<Command> onProcessUserLifecycle(@NotNull ProcessUserLifecycle command) {
        UserLifecycleEvent event = command.event;
        String eventType = event.eventType();
        String userId = event.payload() != null ? event.payload().userId() : null;

        log.info("Processing user lifecycle event: type={}, userId={}", eventType, userId);

        if (!event.success() || userId == null || userId.isBlank()) {
            return this;
        }

        switch (eventType) {
            case "UserRegistered", "UserUpdated" -> {
                User user = new User(userId, event.payload().email(), event.payload().fullName());
                users.put(userId, user);
                log.info("User updated in map: {}", user);
            }
            case "UserDeleted" -> {
                users.remove(userId);
                log.info("User removed from map: userId={}", userId);
            }
            default -> log.debug("Ignoring user lifecycle event type: {}", eventType);
        }

        return this;
    }

    private void handleRegister(@NotNull UserRequestEvent event) {
        String email = event.payload().email();
        String fullName = event.payload().fullName();

        if (isEmpty(email) || isEmpty(fullName)) {
            log.warn("Registration failed: missing email or fullName");
            sendRejection(event, "Missing required fields: email and fullName are required");
            return;
        }

        boolean emailExists = users.values().stream().anyMatch(user -> email.equals(user.email()));
        if (emailExists) {
            log.warn("Registration failed: email {} already exists", email);
            sendRejection(event, "Email already registered");
            return;
        }

        // Generate a short random userId
        String userId = "u_" + java.util.UUID.randomUUID().toString().substring(0, 8);
        User newUser = new User(userId, email, fullName);

        log.info("User registered: {}", newUser);
        sendEvent(OUTPUT_TOPIC, UserLifecycleEvent.createRegistered(event.eventId(), newUser.userId(), newUser.email(), newUser.fullName()));
    }

    private void handleUpdate(@NotNull UserRequestEvent event) {
        String userId = event.payload().userId();
        String email = event.payload().email();
        String fullName = event.payload().fullName();

        if (isEmpty(userId)) {
            log.warn("Update failed: missing userId");
            sendRejection(event, "Missing required field: userId");
            return;
        }

        if (isEmpty(email) || isEmpty(fullName)) {
            log.warn("Update failed: missing email or fullName");
            sendRejection(event, "Missing required fields: email and fullName");
            return;
        }

        if (!users.containsKey(userId)) {
            log.warn("Update failed: user {} not found", userId);
            sendRejection(event, "User not found");
            return;
        }

        // Reject if the new email is already taken by a different user
        boolean emailUsedByOther = users.values().stream()
                .anyMatch(user -> email.equals(user.email()) && !userId.equals(user.userId()));
        if (emailUsedByOther) {
            log.warn("Update failed: email {} already used by another user", email);
            sendRejection(event, "Email already used by another user");
            return;
        }

        User updatedUser = new User(userId, email, fullName);
        log.info("User updated: {}", updatedUser);
        sendEvent(OUTPUT_TOPIC, UserLifecycleEvent.createUpdated(event.eventId(), updatedUser.userId(), updatedUser.email(), updatedUser.fullName()));
    }

    private void handleRemove(@NotNull UserRequestEvent event) {
        String userId = event.payload().userId();

        if (isEmpty(userId)) {
            log.warn("Remove failed: missing userId");
            sendRejection(event, "Missing required field: userId");
            return;
        }

        if (!users.containsKey(userId)) {
            log.warn("Remove failed: user {} not found", userId);
            sendRejection(event, "User not found");
            return;
        }

        log.info("User removed: userId={}", userId);
        sendEvent(OUTPUT_TOPIC, UserLifecycleEvent.createRemoved(event.eventId(), userId));
    }

    private boolean isEmpty(String value) {
        return value == null || value.isBlank();
    }

    private void sendRejection(UserRequestEvent event, String reason) {
        sendEvent(OUTPUT_TOPIC, UserLifecycleEvent.createRejected(event.eventId(), event.eventType(), reason));
    }

    @Override
    protected Behavior<Command> onPreRestart() {
        log.warn("AccountService restarting. Users in memory: {}", users.size());
        return super.onPreRestart();
    }

    @Override
    protected Behavior<Command> onPostStop() {
        log.info("AccountService stopped. Total users managed: {}", users.size());
        return super.onPostStop();
    }

    public interface Command {
    }

    public record ProcessUserRequest(UserRequestEvent event) implements Command {
    }

    public record ProcessUserLifecycle(UserLifecycleEvent event) implements Command {
    }
}
