package common.events;

import org.jetbrains.annotations.NotNull;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

/**
 * Response event emitted by AccountService after a user register/update/remove operation.
 * Event types: "UserRegistered", "UserUpdated", "UserRemoved", or "...Rejected" on failure.
 */
public record UserLifecycleEvent(String eventId,
                                 String eventType,
                                 String timestamp,
                                 String serviceId,
                                 ResponsePayload payload,
                                 boolean success,
                                 String reason
) implements ResponseEvent {

    public static @NotNull UserLifecycleEvent createRegistered(String requestEventId, String userId, String email, String fullName) {
        ResponsePayload payload = new ResponsePayload(userId, email, fullName);
        return new UserLifecycleEvent(requestEventId, "UserRegistered", String.valueOf(System.currentTimeMillis()), "AccountService", payload, true, null);
    }

    public static @NotNull UserLifecycleEvent createUpdated(String requestEventId, String userId, String email, String fullName) {
        ResponsePayload payload = new ResponsePayload(userId, email, fullName);
        return new UserLifecycleEvent(requestEventId, "UserUpdated", String.valueOf(System.currentTimeMillis()), "AccountService", payload, true, null);
    }

    public static @NotNull UserLifecycleEvent createRemoved(String requestEventId, String userId) {
        ResponsePayload payload = new ResponsePayload(userId, null, null);
        return new UserLifecycleEvent(requestEventId, "UserRemoved", String.valueOf(System.currentTimeMillis()), "AccountService", payload, true, null);
    }

    /** The eventType is derived by replacing "Request" with "Rejected" in the original request type. */
    public static @NotNull UserLifecycleEvent createRejected(String requestEventId, @NotNull String eventType, String reason) {
        return new UserLifecycleEvent(requestEventId, eventType.replace("Request", "Rejected"), String.valueOf(System.currentTimeMillis()), "AccountService", null, false, reason);
    }

    /**
     * Parses a UserLifecycleEvent from JSON.
     */
    public static @NotNull UserLifecycleEvent fromJson(String json) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            JsonNode root = mapper.readTree(json);

            String eventId = root.has("eventId") ? root.get("eventId").asString() : "unknown";
            String eventType = root.has("eventType") ? root.get("eventType").asString() : "unknown";
            String timestamp = root.has("timestamp") ? root.get("timestamp").asString() : String.valueOf(System.currentTimeMillis());
            String serviceId = root.has("serviceId") ? root.get("serviceId").asString() : "unknown";
            boolean success = !root.has("success") || root.get("success").asBoolean();
            String reason = root.has("reason") ? root.get("reason").asString() : null;

            ResponsePayload payload = null;
            if (root.has("payload") && !root.get("payload").isNull()) {
                JsonNode p = root.get("payload");
                String userId = p.has("userId") ? p.get("userId").asString() : null;
                String email = p.has("email") ? p.get("email").asString() : null;
                String fullName = p.has("fullName") ? p.get("fullName").asString() : null;
                payload = new ResponsePayload(userId, email, fullName);
            }

            return new UserLifecycleEvent(eventId, eventType, timestamp, serviceId, payload, success, reason);

        } catch (Exception e) {
            throw new RuntimeException("Failed to parse UserLifecycleEvent from JSON: " + json, e);
        }
    }

    @Override
    public String toJson() {
        try {
            ObjectMapper mapper = new ObjectMapper();
            return mapper.writeValueAsString(this);
        } catch (Exception e) {
            throw new RuntimeException("Failed to convert UserLifecycleEvent to JSON", e);
        }
    }

    /**
     * @param userId   user's ID
     * @param email    user's email, null for remove operations
     * @param fullName user's full name, null for remove operations
     */
    public record ResponsePayload(String userId, String email, String fullName) {
    }
}
