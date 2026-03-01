package DistrictNodeManager;

import common.events.RequestEvent;
import org.jetbrains.annotations.NotNull;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

/**
 * Inbound request event for node management operations
 * Supported event types: "NodeCreateRequest", "NodeUpdateRequest", "NodeDeleteRequest"
 *
 * @param eventId   unique request ID
 * @param eventType type of request
 * @param timestamp request creation time
 * @param serviceId originating service
 * @param payload   request data
 */
public record NodeRequestEvent(
        String eventId,
        String eventType,
        String timestamp,
        String serviceId,
        RequestPayload payload
) implements RequestEvent {

    /**
     * Parses a NodeRequestEvent from JSON.
     */
    public static @NotNull NodeRequestEvent fromJson(String json) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            JsonNode root = mapper.readTree(json);

            String eventId = root.has("eventId") ? root.get("eventId").asString() : "unknown";
            String eventType = root.get("eventType").asString();
            String timestamp = root.has("timestamp") ? root.get("timestamp").asString()
                    : String.valueOf(System.currentTimeMillis());
            String serviceId = root.has("serviceId") ? root.get("serviceId").asString() : "unknown";

            JsonNode payloadNode = root.get("payload");
            String userId = payloadNode.has("userId") ? payloadNode.get("userId").asString() : null;
            String nodeId = payloadNode.has("nodeId") ? payloadNode.get("nodeId").asString() : null;
            String districtId = payloadNode.has("districtId") ? payloadNode.get("districtId").asString() : null;
            String nodeType = payloadNode.has("nodeType") ? payloadNode.get("nodeType").asString() : null;

            RequestPayload payload = new RequestPayload(userId, nodeId, districtId, nodeType);
            return new NodeRequestEvent(eventId, eventType, timestamp, serviceId, payload);

        } catch (Exception e) {
            throw new RuntimeException("Failed to parse NodeRequestEvent from JSON: " + json, e);
        }
    }

    @Override
    public String toJson() {
        try {
            ObjectMapper mapper = new ObjectMapper();
            return mapper.writeValueAsString(this);
        } catch (Exception e) {
            throw new RuntimeException("Failed to convert NodeRequestEvent to JSON", e);
        }
    }

    /**
     * @param userId     required for create; used for ownership checks on update/delete
     * @param nodeId     required for update/delete
     * @param districtId required for create, optional for update
     * @param nodeType   required for create, optional for update
     */
    public record RequestPayload(String userId, String nodeId, String districtId, String nodeType) {
    }
}
