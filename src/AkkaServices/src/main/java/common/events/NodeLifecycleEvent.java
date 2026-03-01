package common.events;

import org.jetbrains.annotations.NotNull;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.node.ObjectNode;

/**
 * Response event emitted by DistrictNodeManager after a node create/update/delete operation.
 * Event types: "NodeCreated", "NodeUpdated", "NodeDeleted", or the corresponding "...Rejected" variants.
 */
public record NodeLifecycleEvent(
        String eventId,
        String eventType,
        String timestamp,
        String serviceId,
        Payload payload,
        boolean success,
        String reason
) implements ResponseEvent {

    public static @NotNull NodeLifecycleEvent createNodeCreated(String requestEventId, @NotNull String nodeId, String userId, String districtId, String nodeType) {
        Payload payload = new Payload(nodeId, userId, districtId, nodeType);
        return new NodeLifecycleEvent(requestEventId, "NodeCreated", String.valueOf(System.currentTimeMillis()), "DistrictNodeManager", payload, true, null);
    }

    public static @NotNull NodeLifecycleEvent createNodeUpdated(String requestEventId, @NotNull String nodeId, String userId, String districtId, String nodeType) {
        Payload payload = new Payload(nodeId, userId, districtId, nodeType);
        return new NodeLifecycleEvent(requestEventId, "NodeUpdated", String.valueOf(System.currentTimeMillis()), "DistrictNodeManager", payload, true, null);
    }

    public static @NotNull NodeLifecycleEvent createNodeDeleted(String requestEventId, @NotNull String nodeId) {
        Payload payload = new Payload(nodeId, null, null, null);
        return new NodeLifecycleEvent(requestEventId, "NodeDeleted", String.valueOf(System.currentTimeMillis()), "DistrictNodeManager", payload, true, null);
    }

    public static @NotNull NodeLifecycleEvent createRejected(String requestEventId, @NotNull String requestEventType, String reason) {
        return new NodeLifecycleEvent(
                requestEventId,
                requestEventType.replace("Request", "Rejected"),
                String.valueOf(System.currentTimeMillis()),
                "DistrictNodeManager",
                null,
                false,
                reason
        );
    }

    public static @NotNull NodeLifecycleEvent createNodeCreated(String requestEventId, @NotNull DistrictNodeManager.Node node) {
        return createNodeCreated(requestEventId, node.nodeId(), node.userId(), node.districtId(), node.nodeType());
    }

    public static @NotNull NodeLifecycleEvent createNodeUpdated(String requestEventId, @NotNull DistrictNodeManager.Node node) {
        return createNodeUpdated(requestEventId, node.nodeId(), node.userId(), node.districtId(), node.nodeType());
    }

    /**
     * Parses a NodeLifecycleEvent from JSON.
     */
    public static @NotNull NodeLifecycleEvent fromJson(String json) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            JsonNode root = mapper.readTree(json);

            String eventId = root.has("eventId") ? root.get("eventId").asString() : "unknown";
            String eventType = root.has("eventType") ? root.get("eventType").asString() : "unknown";
            String timestamp = root.has("timestamp") ? root.get("timestamp").asString() : null;
            String serviceId = root.has("serviceId") ? root.get("serviceId").asString() : null;
            boolean success = root.has("success") && root.get("success").asBoolean();
            String reason = root.has("reason") ? root.get("reason").asString() : null;

            Payload payload = null;
            if (root.has("payload") && !root.get("payload").isNull()) {
                JsonNode p = root.get("payload");
                String nodeId = p.has("nodeId") ? p.get("nodeId").asString() : null;
                String userId = p.has("userId") ? p.get("userId").asString() : null;
                String districtId = p.has("districtId") ? p.get("districtId").asString() : null;
                String nodeType = p.has("nodeType") ? p.get("nodeType").asString() : null;
                payload = new Payload(nodeId, userId, districtId, nodeType);
            }

            return new NodeLifecycleEvent(eventId, eventType, timestamp, serviceId, payload, success, reason);
        } catch (Exception e) {
            throw new RuntimeException("Failed to parse NodeLifecycleEvent from JSON: " + json, e);
        }
    }

    @Override
    public String toJson() {
        try {
            ObjectMapper mapper = new ObjectMapper();
            ObjectNode root = mapper.createObjectNode();

            root.put("eventId", eventId);
            root.put("eventType", eventType);
            if (timestamp != null) root.put("timestamp", timestamp);
            if (serviceId != null) root.put("serviceId", serviceId);
            root.put("success", success);

            if (payload != null) {
                ObjectNode payloadNode = mapper.createObjectNode();
                if (payload.nodeId() != null) payloadNode.put("nodeId", payload.nodeId());
                if (payload.userId() != null) payloadNode.put("userId", payload.userId());
                if (payload.districtId() != null) payloadNode.put("districtId", payload.districtId());
                if (payload.nodeType() != null) payloadNode.put("nodeType", payload.nodeType());
                root.set("payload", payloadNode);
            } else {
                root.putNull("payload");
            }

            if (reason != null) {
                root.put("reason", reason);
            }

            return mapper.writeValueAsString(root);
        } catch (Exception e) {
            throw new RuntimeException("Failed to convert NodeLifecycleEvent to JSON", e);
        }
    }

    public record Payload(String nodeId, String userId, String districtId, String nodeType) {
    }
}
