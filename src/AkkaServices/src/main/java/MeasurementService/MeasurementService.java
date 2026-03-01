package MeasurementService;

import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.ReceiveBuilder;
import common.BaseServiceActor;
import common.events.MeasurementEvent;
import common.events.NodeLifecycleEvent;
import common.kafka.KafkaService;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Validates and enriches raw measurements coming from Node-RED/Cooja.
 * Node metadata (owner, district, type) is maintained by consuming node lifecycle events.
 * Valid measurements are published as MeasurementReported events, while invalid ones as MeasurementRejected.
 */
public class MeasurementService extends BaseServiceActor<MeasurementService.Command> {

    private static final String OUTPUT_TOPIC = "kafka.topics.measurement-events";

    // known nodes keyed by nodeId, populated from node lifecycle events
    private final Map<String, NodeInfo> nodes = new HashMap<>();

    private MeasurementService(ActorContext<Command> context, KafkaService kafka) {
        super(context, kafka);
    }

    public static Behavior<Command> create(KafkaService kafka) {
        return BaseServiceActor.create(context -> new MeasurementService(context, kafka));
    }

    @Override
    protected String getServiceName() {
        return "MeasurementService";
    }

    @Override
    protected void addMessageHandlers(ReceiveBuilder<Command> builder) {
        builder.onMessage(ProcessRawMeasurement.class, this::onProcessRawMeasurement);
        builder.onMessage(ProcessNodeLifecycle.class, this::onProcessNodeLifecycle);
        builder.onMessage(GetKnownNodeCount.class, this::onGetKnownNodeCount);
    }

    /**
     * Keeps the node registry consistent with DistrictNodeManager lifecycle events.
     */
    private Behavior<Command> onProcessNodeLifecycle(@NotNull ProcessNodeLifecycle command) {
        NodeLifecycleEvent event = command.event;

        if (!event.success()) {
            return this;
        }

        if (event.payload() == null || isEmpty(event.payload().nodeId())) {
            log.warn("Node lifecycle event missing payload/nodeId, ignoring");
            return this;
        }

        String nodeId = event.payload().nodeId();

        switch (event.eventType()) {
            case "NodeCreated", "NodeUpdated" -> {
                NodeInfo info = new NodeInfo(
                        nodeId,
                        event.payload().userId(),
                        event.payload().districtId(),
                        event.payload().nodeType()
                );
                nodes.put(nodeId, info);
                log.info("Node info upserted: {} (known nodes: {})", nodeId, nodes.size());
            }
            case "NodeDeleted" -> {
                nodes.remove(nodeId);
                log.info("Node removed from registry: {} (known nodes: {})", nodeId, nodes.size());
            }
            default -> { /* other event types are ignored */ }
        }

        return this;
    }

    /**
     * Validates a raw measurement and, if valid, enriches it with node metadata before publishing.
     * Rejects measurements with missing fields, unknown nodes, or incorrect sign for the node type.
     */
    private Behavior<Command> onProcessRawMeasurement(@NotNull ProcessRawMeasurement command) {
        MeasurementRawEvent raw = command.event;

        String nodeId = raw.nodeId();
        Double value = raw.value();
        String measuredAt = raw.timestamp();

        String derivedEventId = "m_" + (nodeId == null ? "unknown" : nodeId) + "_" + (measuredAt == null ? "0" : measuredAt);

        if (isEmpty(nodeId)) {
            log.warn("Rejecting measurement: missing nodeId");
            sendEvent(OUTPUT_TOPIC, MeasurementEvent.createRejected(derivedEventId, "Missing required field: nodeId"));
            return this;
        }

        if (value == null) {
            log.warn("Rejecting measurement: missing value (nodeId={})", nodeId);
            sendEvent(OUTPUT_TOPIC, MeasurementEvent.createRejected(derivedEventId, "Missing required field: value"));
            return this;
        }

        if (isEmpty(measuredAt)) {
            measuredAt = String.valueOf(System.currentTimeMillis());
        }

        NodeInfo info = nodes.get(nodeId);
        if (info == null || isEmpty(info.userId) || isEmpty(info.districtId) || isEmpty(info.nodeType)) {
            log.warn("Rejecting measurement: unknown/unregistered node {}", nodeId);
            sendEvent(OUTPUT_TOPIC, MeasurementEvent.createRejected(derivedEventId, "Unknown nodeId (node not registered)"));
            return this;
        }

        String nodeType = info.nodeType.toLowerCase();

        // Producers report positive values and consumers report negative values
        if ("producer".equals(nodeType) && value < 0) {
            log.warn("Rejecting measurement: producer node {} sent negative value {}", nodeId, value);
            sendEvent(OUTPUT_TOPIC, MeasurementEvent.createRejected(derivedEventId, "Producer measurement must be positive"));
            return this;
        }

        if ("consumer".equals(nodeType) && value > 0) {
            log.warn("Rejecting measurement: consumer node {} sent positive value {}", nodeId, value);
            sendEvent(OUTPUT_TOPIC, MeasurementEvent.createRejected(derivedEventId, "Consumer measurement must be negative"));
            return this;
        }

        // Pick a random accumulator in the same district (null if none exists)
        String accumulatorNodeId = null;
        if (!("accumulator".equals(nodeType))) {
            accumulatorNodeId = chooseAccumulatorForDistrict(info.districtId);
        }

        MeasurementEvent event = MeasurementEvent.createReported(
                derivedEventId,
                nodeId,
                info.nodeType,
                info.districtId,
                info.userId,
                value,
                measuredAt,
                accumulatorNodeId
        );

        log.info("Measurement accepted: nodeId={}, value={}, nodeType={}, userId={}, districtId={}, accumulatorNodeId={}",
                nodeId, value, info.nodeType, info.userId, info.districtId, accumulatorNodeId);
        sendEvent(OUTPUT_TOPIC, event);

        return this;
    }

    private Behavior<Command> onGetKnownNodeCount(@NotNull GetKnownNodeCount command) {
        command.replyTo.tell(new KnownNodeCountResponse(nodes.size()));
        return this;
    }

    private boolean isEmpty(String value) {
        return value == null || value.isBlank();
    }

    private String chooseAccumulatorForDistrict(@NotNull String districtId) {
        if (isEmpty(districtId)) {
            return null;
        }

        List<String> accumulatorNodeIds = new ArrayList<>();
        for (NodeInfo info : nodes.values()) {
            if (info == null) continue;
            if (isEmpty(info.nodeId) || isEmpty(info.districtId) || isEmpty(info.nodeType)) continue;
            if (!districtId.equals(info.districtId)) continue;
            if (!"accumulator".equalsIgnoreCase(info.nodeType)) continue;
            accumulatorNodeIds.add(info.nodeId);
        }

        if (accumulatorNodeIds.isEmpty()) {
            return null;
        }

        int idx = ThreadLocalRandom.current().nextInt(accumulatorNodeIds.size());
        return accumulatorNodeIds.get(idx);
    }

    @Override
    protected Behavior<Command> onPreRestart() {
        log.warn("MeasurementService restarting. Known nodes: {}", nodes.size());
        return super.onPreRestart();
    }

    @Override
    protected Behavior<Command> onPostStop() {
        log.info("MeasurementService stopped. Known nodes: {}", nodes.size());
        return super.onPostStop();
    }

    public interface Command {
    }

    public record ProcessRawMeasurement(MeasurementRawEvent event) implements Command {
    }

    public record ProcessNodeLifecycle(NodeLifecycleEvent event) implements Command {
    }

    public record GetKnownNodeCount(akka.actor.typed.ActorRef<KnownNodeCountResponse> replyTo) implements Command {
    }

    public record KnownNodeCountResponse(int count) {
    }

    private record NodeInfo(String nodeId, String userId, String districtId, String nodeType) {
    }
}
