package PingPongService;

import common.events.ResponseEvent;
import org.jetbrains.annotations.NotNull;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.node.ObjectNode;

import java.util.UUID;

/**
 * Response event sent by PingPongService when a ping is received.
 */
public record PongEvent(
        String eventId,
        String eventType,
        String timestamp,
        String serviceId,
        Payload payload,
        boolean success,
        String reason
) implements ResponseEvent {

    public static @NotNull PongEvent createPong(@NotNull String requestMessage) {
        Payload payload = new Payload(requestMessage, "pong");
        return new PongEvent(
                "pong_" + UUID.randomUUID().toString().substring(0, 8),
                "PongSent",
                String.valueOf(System.currentTimeMillis()),
                "PingPongService",
                payload,
                true,
                null
        );
    }

    public static @NotNull PongEvent createRejected(String eventId, String reason) {
        return new PongEvent(
                eventId,
                "PongRejected",
                String.valueOf(System.currentTimeMillis()),
                "PingPongService",
                null,
                false,
                reason
        );
    }

    @Override
    public String toJson() {
        try {
            ObjectMapper mapper = new ObjectMapper();
            ObjectNode root = mapper.createObjectNode();

            root.put("eventId", eventId);
            root.put("eventType", eventType);
            root.put("timestamp", timestamp);
            root.put("serviceId", serviceId);
            root.put("success", success);

            if (payload != null) {
                ObjectNode payloadNode = mapper.createObjectNode();
                payloadNode.put("request", payload.request());
                payloadNode.put("response", payload.response());
                root.set("payload", payloadNode);
            }

            if (reason != null) {
                root.put("reason", reason);
            }

            return mapper.writeValueAsString(root);
        } catch (Exception e) {
            throw new RuntimeException("Failed to convert PongEvent to JSON", e);
        }
    }

    public record Payload(String request, String response) {
    }
}
