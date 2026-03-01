package BillingService;

import common.events.ResponseEvent;
import org.jetbrains.annotations.NotNull;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.node.ObjectNode;

/**
 * Periodic usage and cost summary emitted by BillingService for a specific time window.
 * Event types: "UsageRecordCreated" or "UsageRecordRejected".
 */
public record UsageRecordEvent(String eventId,
                               String eventType,
                               String timestamp,
                               String serviceId,
                               Payload payload,
                               boolean success,
                               String reason
) implements ResponseEvent {

    public static @NotNull UsageRecordEvent createUsageRecord(String eventId, @NotNull String userId, @NotNull String districtId, long windowStart, long windowEnd, double produced, double consumed, double netBalance, double cost) {
        Payload payload = new Payload(userId, districtId, windowStart, windowEnd, produced, consumed, netBalance, cost);
        return new UsageRecordEvent(eventId, "UsageRecordCreated", String.valueOf(System.currentTimeMillis()), "BillingService", payload, true, null);
    }

    public static @NotNull UsageRecordEvent createRejected(String eventId, String reason) {
        return new UsageRecordEvent(eventId, "UsageRecordRejected", String.valueOf(System.currentTimeMillis()), "BillingService", null, false, reason);
    }

    /**
     * Parses a UsageRecordEvent from JSON.
     */
    public static @NotNull UsageRecordEvent fromJson(String json) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            JsonNode root = mapper.readTree(json);

            String eventId = root.has("eventId") ? root.get("eventId").asString() : "unknown";
            String eventType = root.get("eventType").asString();
            String timestamp = root.has("timestamp") ? root.get("timestamp").asString() : String.valueOf(System.currentTimeMillis());
            String serviceId = root.has("serviceId") ? root.get("serviceId").asString() : "unknown";
            boolean success = root.has("success") && root.get("success").asBoolean();

            Payload payload = null;
            if (root.has("payload") && !root.get("payload").isNull()) {
                JsonNode p = root.get("payload");
                String userId = p.has("userId") ? p.get("userId").asString() : null;
                String districtId = p.has("districtId") ? p.get("districtId").asString() : null;
                long windowStart = p.has("windowStart") ? p.get("windowStart").asLong() : 0L;
                long windowEnd = p.has("windowEnd") ? p.get("windowEnd").asLong() : 0L;
                double produced = p.has("produced") ? p.get("produced").asDouble() : 0.0;
                double consumed = p.has("consumed") ? p.get("consumed").asDouble() : 0.0;
                double netBalance = p.has("netBalance") ? p.get("netBalance").asDouble() : 0.0;
                double cost = p.has("cost") ? p.get("cost").asDouble() : 0.0;
                payload = new Payload(userId, districtId, windowStart, windowEnd, produced, consumed, netBalance, cost);
            }

            String reason = root.has("reason") ? root.get("reason").asString() : null;

            return new UsageRecordEvent(eventId, eventType, timestamp, serviceId, payload, success, reason);
        } catch (Exception e) {
            throw new RuntimeException("Failed to parse UsageRecordEvent from JSON: " + json, e);
        }
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
                ObjectNode p = mapper.createObjectNode();
                p.put("userId", payload.userId());
                p.put("districtId", payload.districtId());
                p.put("windowStart", payload.windowStart());
                p.put("windowEnd", payload.windowEnd());
                p.put("produced", payload.produced());
                p.put("consumed", payload.consumed());
                p.put("netBalance", payload.netBalance());
                p.put("cost", payload.cost());
                root.set("payload", p);
            }

            if (reason != null) {
                root.put("reason", reason);
            }

            return mapper.writeValueAsString(root);
        } catch (Exception e) {
            throw new RuntimeException("Failed to convert UsageRecordEvent to JSON", e);
        }
    }

    public record Payload(String userId, String districtId, long windowStart, long windowEnd, double produced,
                          double consumed, double netBalance, double cost) {
    }
}
