package AccountService;

import common.events.RequestEvent;
import org.jetbrains.annotations.NotNull;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

/**
 * Inbound request event for user management operations.
 * Supported event types: "UserRegisterRequest", "UserUpdateRequest", "UserRemoveRequest".
 *
 * @param eventId   unique request ID
 * @param eventType type of request
 * @param timestamp request creation time
 * @param serviceId originating service
 * @param payload   request data
 */
public record UserRequestEvent(String eventId, String eventType, String timestamp, String serviceId,
                               RequestPayload payload) implements RequestEvent {

    /**
     * Parses a UserRequestEvent from JSON.
     */
    public static @NotNull UserRequestEvent fromJson(String json) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            JsonNode root = mapper.readTree(json);

            String eventId = root.has("eventId") ? root.get("eventId").asString() : "unknown";
            String eventType = root.get("eventType").asString();
            String timestamp = root.has("timestamp") ? root.get("timestamp").asString() : String.valueOf(System.currentTimeMillis());
            String serviceId = root.has("serviceId") ? root.get("serviceId").asString() : "unknown";

            JsonNode payloadNode = root.get("payload");
            String email = payloadNode.has("email") ? payloadNode.get("email").asString() : null;
            String fullName = payloadNode.has("fullName") ? payloadNode.get("fullName").asString() : null;
            String userId = payloadNode.has("userId") ? payloadNode.get("userId").asString() : null;

            RequestPayload payload = new RequestPayload(email, fullName, userId);
            return new UserRequestEvent(eventId, eventType, timestamp, serviceId, payload);

        } catch (Exception e) {
            throw new RuntimeException("Failed to parse UserRequestEvent from JSON: " + json, e);
        }
    }

    @Override
    public String toJson() {
        try {
            ObjectMapper mapper = new ObjectMapper();
            return mapper.writeValueAsString(this);
        } catch (Exception e) {
            throw new RuntimeException("Failed to convert UserRequestEvent to JSON", e);
        }
    }

    /**
     * @param email    required for register/update
     * @param fullName required for register/update
     * @param userId   required for update/remove
     */
    public record RequestPayload(String email, String fullName, String userId) {
    }
}
