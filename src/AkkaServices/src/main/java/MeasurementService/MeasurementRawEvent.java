package MeasurementService;

import org.jetbrains.annotations.NotNull;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

/**
 * Raw measurement event produced by Node-RED/Cooja nodes, consumed before enrichment.
 *
 * @param nodeId    ID of the reporting node
 * @param value     measured value (positive for producers, negative for consumers)
 * @param timestamp measurement time in milliseconds
 */
public record MeasurementRawEvent(String nodeId, Double value, String timestamp) {

    /**
     * Parses a raw measurement from JSON.
     */
    public static @NotNull MeasurementRawEvent fromJson(String json) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            JsonNode root = mapper.readTree(json);

            String nodeId = root.has("nodeId") ? root.get("nodeId").asString() : null;

            Double value = null;
            if (root.has("value") && !root.get("value").isNull()) {
                value = root.get("value").asDouble();
            } else if (root.has("measurement") && !root.get("measurement").isNull()) {
                value = root.get("measurement").asDouble();
            }

            String ts;
            if (root.has("timestamp")) {
                ts = root.get("timestamp").asString();
            } else if (root.has("measuredAt")) {
                ts = root.get("measuredAt").asString();
            } else {
                ts = String.valueOf(System.currentTimeMillis());
            }

            return new MeasurementRawEvent(nodeId, value, ts);
        } catch (Exception e) {
            throw new RuntimeException("Failed to parse MeasurementRawEvent from JSON: " + json, e);
        }
    }

    public String toJson() {
        try {
            ObjectMapper mapper = new ObjectMapper();
            return mapper.writeValueAsString(this);
        } catch (Exception e) {
            throw new RuntimeException("Failed to convert MeasurementRawEvent to JSON", e);
        }
    }
}
