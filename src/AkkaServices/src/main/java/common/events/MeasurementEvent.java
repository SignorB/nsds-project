package common.events;

import org.jetbrains.annotations.NotNull;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.node.ObjectNode;

/**
 * Response event emitted by MeasurementService after validating a raw measurement.
 * Event types: "MeasurementReported" (valid) or "MeasurementRejected" (invalid).
 */
public record MeasurementEvent(
        String eventId,
        String eventType,
        String timestamp,
        String serviceId,
        Payload payload,
        boolean success,
        String reason
) implements ResponseEvent {

    public static @NotNull MeasurementEvent createReported(
            String eventId,
            @NotNull String nodeId,
            @NotNull String nodeType,
            @NotNull String districtId,
            @NotNull String userId,
            double value,
            @NotNull String measuredAt,
            String accumulatorNodeId
    ) {
        Payload payload = new Payload(nodeId, nodeType, districtId, userId, value, measuredAt, accumulatorNodeId);
        return new MeasurementEvent(
                eventId,
                "MeasurementReported",
                String.valueOf(System.currentTimeMillis()),
                "MeasurementService",
                payload,
                true,
                null
        );
    }

    public static @NotNull MeasurementEvent createRejected(String eventId, String reason) {
        return new MeasurementEvent(
                eventId,
                "MeasurementRejected",
                String.valueOf(System.currentTimeMillis()),
                "MeasurementService",
                null,
                false,
                reason
        );
    }

    /**
     * Parses a MeasurementEvent from its JSON representation.
     */
    public static @NotNull MeasurementEvent fromJson(String json) {
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
                String nodeType = p.has("nodeType") ? p.get("nodeType").asString() : null;
                String districtId = p.has("districtId") ? p.get("districtId").asString() : null;
                String userId = p.has("userId") ? p.get("userId").asString() : null;
                double value = p.has("value") ? p.get("value").asDouble() : 0.0;
                String measuredAt = p.has("measuredAt") ? p.get("measuredAt").asString() : null;
                String accumulatorNodeId = p.has("accumulatorNodeId") ? p.get("accumulatorNodeId").asString() : null;
                payload = new Payload(nodeId, nodeType, districtId, userId, value, measuredAt, accumulatorNodeId);
            }

            return new MeasurementEvent(eventId, eventType, timestamp, serviceId, payload, success, reason);
        } catch (Exception e) {
            throw new RuntimeException("Failed to parse MeasurementEvent from JSON: " + json, e);
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
                if (payload.nodeType() != null) payloadNode.put("nodeType", payload.nodeType());
                if (payload.districtId() != null) payloadNode.put("districtId", payload.districtId());
                if (payload.userId() != null) payloadNode.put("userId", payload.userId());
                payloadNode.put("value", payload.value());
                if (payload.measuredAt() != null) payloadNode.put("measuredAt", payload.measuredAt());
                if (payload.accumulatorNodeId() != null) payloadNode.put("accumulatorNodeId", payload.accumulatorNodeId());
                root.set("payload", payloadNode);
            } else {
                root.putNull("payload");
            }

            if (reason != null) {
                root.put("reason", reason);
            }

            return mapper.writeValueAsString(root);
        } catch (Exception e) {
            throw new RuntimeException("Failed to convert MeasurementEvent to JSON", e);
        }
    }

    public record Payload(
            String nodeId,
            String nodeType,
            String districtId,
            String userId,
            double value,
            String measuredAt,
            String accumulatorNodeId
    ) {
    }
}
